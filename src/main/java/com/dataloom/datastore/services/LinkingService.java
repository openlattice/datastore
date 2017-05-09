package com.dataloom.datastore.services;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.LoomUtil;
import com.dataloom.data.DataGraphManager;
import com.dataloom.data.DatasourceManager;
import com.dataloom.data.EntityDatastore;
import com.dataloom.data.EntityKey;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.datastore.services.EdmManager;

public class LinkingService {
    private static final Logger           logger = LoggerFactory.getLogger( LinkingService.class );

    private final ObjectMapper mapper;
    private final HazelcastLinkingGraphs   linkingGraph;
    private final Blocker                  blocker;
    private final Matcher                  matcher;
    private final Clusterer                clusterer;
    private final HazelcastListingService  listingService;
    private final EdmManager               dms;
    private final EntityDatastore eds;
    private final DataGraphManager dgm;
    private final DatasourceManager        dsm;
    private final String                   keyspace;
    private final Session                  session;

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
            DataGraphManager dgm,
            DatasourceManager dsm,
            EntityDatastore eds,
            ObjectMapper mapper ) {
        this.linkingGraph = linkingGraph;

        this.blocker = blocker;
        this.matcher = matcher;
        this.clusterer = clusterer;

        this.session = session;
        this.keyspace = keyspace;

        eventBus.register( this );

        this.listingService = listingService;
        this.dms = dms;
        this.dgm = dgm;
        this.dsm = dsm;
        this.eds = eds;
        this.mapper = mapper;
    }

    public UUID link( UUID linkedEntitySetId, Set<Map<UUID, UUID>> linkingProperties, Set<UUID> ownablePropertyTypes ) {
        SetMultimap<UUID, UUID> linkIndexedByPropertyTypes = getLinkIndexedByPropertyTypes( linkingProperties );
        SetMultimap<UUID, UUID> linkIndexedByEntitySets = getLinkIndexedByEntitySets( linkingProperties );

        // Warning: We assume that the restrictions on links are enforced/validated as specified in LinkingApi. In
        // particular, from now on we work on the assumption that only identical property types are linked on.
        initializeComponents( dsm.getCurrentSyncId( linkIndexedByEntitySets.keySet() ), linkIndexedByPropertyTypes, linkIndexedByEntitySets );

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
                    if ( entityPair.getBackingCollection().size() == 2 ) {
                        // The pair actually consists of two entities; we should add the edge to the graph if necessary.
                        final LinkingEdge edge = fromUnorderedPair( graphId, entityPair );
                        if ( buffer.tryAddEdge( edge ) ) {
                            double weight = matcher.dist( entityPair );
                            buffer.setEdgeWeight( new WeightedLinkingEdge( weight, edge ) );
                        }
                    } else {
                        // The pair consists of one entity; we should add a vertex to the graph if necessary.
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

        // compute authorized property types for each of the linking entity sets, as well as the linked entity set itself
        Set<UUID> linkingSets = listingService.getLinkedEntitySets( linkedEntitySetId );
        Iterable<UUID> involvedEntitySets = Iterables.concat( linkingSets, ImmutableSet.of( linkedEntitySetId ) );
        for ( UUID esId : involvedEntitySets ) {
            Set<UUID> propertiesOfEntitySet = dms.getEntityTypeByEntitySetId( esId ).getProperties();
            Set<UUID> authorizedProperties = Sets.intersection( ownablePropertyTypes, propertiesOfEntitySet );

            Map<UUID, PropertyType> authorizedPropertyTypes = authorizedProperties.stream()
                    .collect( Collectors.toMap( ptId -> ptId, ptId -> dms.getPropertyType( ptId ) ) );

            authorizedPropertyTypesForEntitySets.put( esId, authorizedPropertyTypes );
        }
        
        UUID syncId = dsm.getCurrentSyncId( linkedEntitySetId );
        
        mergeVertices( linkedEntitySetId, syncId, authorizedPropertyTypesForEntitySets );
        
        mergeEdges( linkedEntitySetId, linkingSets, syncId );
    }

    private void mergeVertices( 
            UUID linkedEntitySetId,
            UUID syncId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets ){
        Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataTypeForLinkedEntitySet = Maps.transformValues( authorizedPropertyTypesForEntitySets.get( linkedEntitySetId ), pt -> pt.getDatatype() );
        // EntityType.getKey returns an unmodifiable view of the underlying linked hash set, so the order is still preserved, although 
        List<UUID> keyProperties = new LinkedList<>( dms.getEntityTypeByEntitySetId( linkedEntitySetId ).getKey() );
        
        Iterable<Pair<UUID, Set<EntityKey>>> linkedEntityKeys = linkingGraph.getLinkedEntityKeys( linkedEntitySetId );
        for( Pair<UUID, Set<EntityKey>> linkedKey : linkedEntityKeys ){
            // compute merged entity
            SetMultimap<UUID, Object> mergedEntityDetails = computeMergedEntity( linkedKey.getValue(), authorizedPropertyTypesForEntitySets );
            String entityId = LoomUtil.generateDefaultEntityId( keyProperties, mergedEntityDetails );

            // create merged entity, in particular get back the entity key id for the new entity
            UUID mergedEntityKeyId = dgm.createEntity( linkedEntitySetId, syncId, entityId, mergedEntityDetails, authorizedPropertiesWithDataTypeForLinkedEntitySet );
            
            // write to a lookup table from old entity key id to new, merged entity key id
            idService.getEntityKeyIds( linkedKey.getValue() ).values().forEach( oldId -> lsgsa.saveToLookup( linkedEntitySetId, syncId, oldId, nmergedEntityKeyId ) );
        }
    }
    
    private void mergeEdges( UUID linkedEntitySetId, Set<UUID> linkingSets, UUID syncId ){
        Map<CommonColumns, Set<UUID>> edgeSelection = new HashMap<>();
        edgeSelection.put( CommonColumns.SRC_ENTITY_SET_ID, linkingSets );
        edgeSelection.put( CommonColumns.DST_ENTITY_SET_ID, linkingSets );
        edgeSelection.put( CommonColumns.EDGE_ENTITY_SET_ID, linkingSets );
        
        lm.getEdges( edgeSelection ).forEach( edge -> mergeEdge( linkedEntitySetId, syncId, edge ) );
    }

    private SetMultimap<UUID, Object> computeMergedEntity( Set<EntityKey> entityKeys, 
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets ){
        SetMultimap<UUID, Object> result = HashMultimap.create();
        
        entityKeys.stream()
        .map( key -> Pair.of( key.getEntitySetId(),
                eds.asyncLoadEntity( key.getEntitySetId(),
                        key.getEntityId(),
                        key.getSyncId(),
                        authorizedPropertyTypesForEntitySets.get( key.getEntitySetId() ).keySet() ) ) )
        .map( rsfPair -> Pair.of( rsfPair.getKey(), rsfPair.getValue().getUninterruptibly() ) )
        .map( rsPair -> RowAdapters.entityIndexedById( rsPair.getValue(),
                authorizedPropertyTypesForEntitySets.get( rsPair.getKey() ),
                mapper ) )
        .forEach( result::putAll );
        
        return result;
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
            Map<UUID, UUID> linkingEntitySetsWithSyncIds,
            SetMultimap<UUID, UUID> linkIndexedByPropertyTypes,
            SetMultimap<UUID, UUID> linkIndexedByEntitySets ) {
        blocker.setLinking( linkingEntitySetsWithSyncIds, linkIndexedByPropertyTypes, linkIndexedByEntitySets );
        matcher.setLinking( linkingEntitySetsWithSyncIds, linkIndexedByPropertyTypes, linkIndexedByEntitySets );
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
