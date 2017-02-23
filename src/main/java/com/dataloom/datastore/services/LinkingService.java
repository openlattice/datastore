package com.dataloom.datastore.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.EntityKey;
import com.dataloom.datasource.UUIDs.Syncs;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.linking.Entity;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.linking.HazelcastListingService;
import com.dataloom.linking.LinkingEdge;
import com.dataloom.linking.LinkingEntityKey;
import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.linking.SortedCassandraLinkingEdgeBuffer;
import com.dataloom.linking.WeightedLinkingEdge;
import com.dataloom.linking.components.Blocker;
import com.dataloom.linking.components.Clusterer;
import com.dataloom.linking.components.Matcher;
import com.dataloom.linking.util.UnorderedPair;
import com.datastax.driver.core.Session;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.datastore.services.EdmManager;

public class LinkingService {
    private static final Logger           logger = LoggerFactory.getLogger( LinkingService.class );

    private final HazelcastLinkingGraphs  linkingGraph;
    private final Blocker                 blocker;
    private final Matcher                 matcher;
    private final Clusterer               clusterer;
    private final HazelcastListingService listingService;
    private final EdmManager              dms;
    private final CassandraDataManager    cdm;
    private final String                  keyspace;
    private final Session                 session;

    public LinkingService(
            String keyspace,
            Session session,
            HazelcastLinkingGraphs linkingGraph,
            Blocker blocker,
            Matcher matcher,
            Clusterer clusterer,
            HazelcastInstance hazelcast,
            EventBus eventBus,
            HazelcastListingService listingService,
            EdmManager dms,
            CassandraDataManager cdm ) {
        this.linkingGraph = linkingGraph;

        this.blocker = blocker;
        this.matcher = matcher;
        this.clusterer = clusterer;

        this.session = session;
        this.keyspace = keyspace;

        eventBus.register( this );

        this.listingService = listingService;
        this.dms = dms;
        this.cdm = cdm;
    }

    public UUID link( UUID linkedEntitySetId, Set<Map<UUID, UUID>> linkingProperties, Set<UUID> ownablePropertyTypes ) {
        SetMultimap<UUID, UUID> linkIndexedByPropertyTypes = getLinkIndexedByPropertyTypes( linkingProperties );
        SetMultimap<UUID, UUID> linkIndexedByEntitySets = getLinkIndexedByEntitySets( linkingProperties );

        // TODO may be deprecated, depending on whether syncIds is required to do reads.
        Map<UUID, UUID> entitySetsWithSyncIds = linkIndexedByEntitySets.keySet().stream()
                .collect( Collectors.toMap( esId -> esId, esId -> Syncs.BASE.getSyncId() ) );

        // Warning: We assume that the restrictions on links are enforced/validated as specified in LinkingApi. In
        // particular, from now on we work on the assumption that only identical property types are linked on.
        initializeComponents( entitySetsWithSyncIds, linkIndexedByPropertyTypes, linkIndexedByEntitySets );

        UUID graphId = linkingGraph.getGraphIdFromEntitySetId( linkedEntitySetId );

        logger.info( "Executing blocking..." );
        // Blocking: For each row in the entity sets, fire off query to elasticsearch
        Stream<UnorderedPair<Entity>> pairs = blocker.block();
        final SortedCassandraLinkingEdgeBuffer buffer = new SortedCassandraLinkingEdgeBuffer(
                keyspace,
                session,
                graphId,
                0.0,
                0,
                0 );

        logger.info( "Executing matching..." );
        // Matching: check if pair score is already calculated from HazelcastGraph Api. If not, stream
        // through matcher to get a score.
        pairs
                .forEach( entityPair -> {
                    if( entityPair.getBackingCollection().size() == 2 ){
                        //The pair actually consists of two entities; we should add the edge to the graph if necessary.
                        final LinkingEdge edge = fromUnorderedPair( graphId, entityPair );
                        if( buffer.tryAddEdge( edge ) ) {
                            double weight = matcher.dist( entityPair );
                            buffer.setEdgeWeight( new WeightedLinkingEdge( weight, edge ) );
                        }
                    } else {
                        //The pair consists of one entity; we should add a vertex to the graph if necessary.
                        final EntityKey ek = getEntityKeyFromSingletonPair( entityPair );
                        linkingGraph.getOrCreateVertex( graphId, ek );
                    }
                } );

        // Feed the scores (i.e. the edge set) into HazelcastGraph Api
        logger.info( "Executing clustering..." );
        clusterer.cluster( graphId );

        mergeEntities( linkedEntitySetId, ownablePropertyTypes );

        logger.info( "Linking job finished." );
        return linkedEntitySetId;
    }

    private void mergeEntities( UUID linkedEntitySetId, Set<UUID> ownablePropertyTypes ) {
        Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets = new HashMap<>();

        for ( UUID esId : listingService.getLinkedEntitySets( linkedEntitySetId ) ) {
            Set<UUID> propertiesOfEntitySet = dms.getEntityTypeByEntitySetId( esId ).getProperties();
            Set<UUID> authorizedProperties = Sets.intersection( ownablePropertyTypes, propertiesOfEntitySet );

            Map<UUID, PropertyType> authorizedPropertyTypes = authorizedProperties.stream()
                    .collect( Collectors.toMap( ptId -> ptId, ptId -> dms.getPropertyType( ptId ) ) );

            authorizedPropertyTypesForEntitySets.put( esId, authorizedPropertyTypes );
        }

        //Consume the iterable to trigger indexing!
        cdm.getLinkedEntitySetData( linkedEntitySetId, authorizedPropertyTypesForEntitySets ).forEach( m -> {} );
    }

    private LinkingEdge fromUnorderedPair( UUID graphId, UnorderedPair<Entity> p ) {
        List<LinkingEntityKey> keys = p.getBackingCollection().stream()
                .map( e -> new LinkingEntityKey( graphId, e.getKey() ) ).collect( Collectors.toList() );
        LinkingVertexKey u = linkingGraph.getOrCreateVertex( keys.get( 0 ) );
        LinkingVertexKey v = linkingGraph.getOrCreateVertex( keys.get( 1 ) );
        return new LinkingEdge( u, v );
    }

    private EntityKey getEntityKeyFromSingletonPair( UnorderedPair<Entity> p ) {
        Entity e = p.getBackingCollection().iterator().next();
        if ( e == null ) {
            logger.error( "Unexpected null singleton entity pair during blocking." );
            throw new IllegalStateException( "Unexpected error during blocking." );
        }
        return e.getKey();
    }

    private void initializeComponents(
            Map<UUID, UUID> entitySetsWithSyncIds,
            SetMultimap<UUID, UUID> linkIndexedByPropertyTypes,
            SetMultimap<UUID, UUID> linkIndexedByEntitySets ) {
        blocker.setLinking( entitySetsWithSyncIds, linkIndexedByPropertyTypes, linkIndexedByEntitySets );
        matcher.setLinking( entitySetsWithSyncIds, linkIndexedByPropertyTypes, linkIndexedByEntitySets );
    }

    /**
     * Utility methods to get various forms/info about links.
     */

    public static SetMultimap<UUID, UUID> getLinkIndexedByPropertyTypes( Set<Map<UUID, UUID>> linkingProperties ) {
        SetMultimap<UUID, UUID> result = HashMultimap.create();

        linkingProperties.stream().flatMap( m -> m.entrySet().stream() )
                .forEach( entry -> result.put( entry.getValue(), entry.getKey() ) );

        return result;
    }

    public static SetMultimap<UUID, UUID> getLinkIndexedByEntitySets( Set<Map<UUID, UUID>> linkingProperties ) {
        SetMultimap<UUID, UUID> result = HashMultimap.create();

        linkingProperties.stream().flatMap( m -> m.entrySet().stream() )
                .forEach( entry -> result.put( entry.getKey(), entry.getValue() ) );

        return result;
    }

    public static Set<UUID> getLinkingSets( Set<Map<UUID, UUID>> linkingProperties ) {
        return getLinkIndexedByEntitySets( linkingProperties ).keySet();
    }

}
