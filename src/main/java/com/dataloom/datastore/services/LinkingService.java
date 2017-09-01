package com.dataloom.datastore.services;

import com.dataloom.LoomUtil;
import com.dataloom.data.DataGraphManager;
import com.dataloom.data.DatasourceManager;
import com.dataloom.data.EntityDatastore;
import com.dataloom.data.EntityKey;
import com.dataloom.data.EntityKeyIdService;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.core.LoomGraphApi;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.linking.CassandraLinkingGraphsQueryService;
import com.dataloom.linking.Entity;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.linking.HazelcastListingService;
import com.dataloom.linking.HazelcastVertexMergingService;
import com.dataloom.linking.LinkingEdge;
import com.dataloom.linking.LinkingEntityKey;
import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.linking.SortedCassandraLinkingEdgeBuffer;
import com.dataloom.linking.WeightedLinkingEdge;
import com.dataloom.linking.components.Blocker;
import com.dataloom.linking.components.Clusterer;
import com.dataloom.linking.components.Matcher;
import com.dataloom.linking.configuration.PersonFeatureWeights;
import com.dataloom.linking.util.FeatureExtractor;
import com.dataloom.linking.util.UnorderedPair;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListenableFuture;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinkingService {
    private static final Logger logger = LoggerFactory.getLogger( LinkingService.class );

    private final ObjectMapper                       mapper;
    private final HazelcastLinkingGraphs             linkingGraph;
    private final Blocker                            blocker;
    private final Matcher                            matcher;
    private final Clusterer                          clusterer;
    private final HazelcastListingService            listingService;
    private final EdmManager                         dms;
    private final DataGraphManager                   dgm;
    private final EntityDatastore                    eds;
    private final LoomGraphApi                       lm;
    private final DatasourceManager                  dsm;
    private final String                             keyspace;
    private final Session                            session;
    private final EntityKeyIdService                 idService;
    private final HazelcastVertexMergingService      vms;
    private final CassandraLinkingGraphsQueryService clgqs;

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
            LoomGraphApi lm,
            EntityKeyIdService idService,
            HazelcastVertexMergingService vms,
            CassandraLinkingGraphsQueryService clgqs,
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
        this.lm = lm;
        this.idService = idService;
        this.vms = vms;
        this.mapper = mapper;
        this.clgqs = clgqs;
    }

    public void link( Set<UUID> entitySetIds ) {

    }

    public UUID link(
            UUID linkedEntitySetId,
            Set<Map<UUID, UUID>> linkingProperties,
            Set<UUID> ownablePropertyTypes,
            Set<UUID> propertyTypesToPopulate ) {
        SetMultimap<UUID, UUID> linkIndexedByPropertyTypes = getLinkIndexedByPropertyTypes( linkingProperties );
        SetMultimap<UUID, UUID> linkIndexedByEntitySets = getLinkIndexedByEntitySets( linkingProperties );
        Map<FullQualifiedName, String> propertyTypeIdIndexedByFqn = getPropertyTypeIdIndexedByFqn( linkingProperties );

        boolean isMatchingPerson = dms.getEntityTypeByEntitySetId( linkIndexedByEntitySets.keySet().iterator().next() )
                .getType().toString().equals( "general.person" );

        // Warning: We assume that the restrictions on links are enforced/validated as specified in LinkingApi. In
        // particular, from now on we work on the assumption that only identical property types are linked on.
        initializeComponents( dsm.getCurrentSyncId( linkIndexedByEntitySets.keySet() ),
                linkIndexedByPropertyTypes,
                linkIndexedByEntitySets );

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
        final PriorityQueue<WeightedLinkingEdge> pq = new PriorityQueue<>( 3, Comparator.reverseOrder() );

        logger.info( "Executing matching..." );
        double[] personWeights = ConfigurationService.StaticLoader.loadConfiguration( PersonFeatureWeights.class )
                .getWeights();
        // Matching: check if pair score is already calculated from HazelcastGraph Api. If not, stream
        // through matcher to get a score.
        pairs
                .forEach( entityPair -> {
                    if ( entityPair.getBackingCollection().size() == 2 ) {
                        // The pair actually consists of two entities; we should add the edge to the graph if necessary.
                        final LinkingEdge edge = fromUnorderedPair( graphId, entityPair );
                        //                        if ( buffer.tryAddEdge( edge ) ) {
                        double weight = ( isMatchingPerson )
                                ? FeatureExtractor.getEntityDiffForWeights( entityPair,
                                personWeights,
                                propertyTypeIdIndexedByFqn )
                                : matcher.dist( entityPair );
                        //                            buffer.setEdgeWeight(
                        pq.add( new WeightedLinkingEdge( weight, edge ) );
                        if ( pq.size() > 2 ) {
                            pq.poll();
                        }
                    } else {
                        // The pair consists of one entity; we should add a vertex to the graph if necessary.
                        final EntityKey ek = getEntityKeyFromSingletonPair( entityPair );
                        linkingGraph.getOrCreateVertex( graphId, ek );
                    }
                } );
        Preconditions.checkState( pq.size() > 0, "Must have at least one edge" );
        WeightedLinkingEdge top = pq.poll();
        WeightedLinkingEdge bottom = pq.poll();
        // Feed the scores (i.e. the edge set) into HazelcastGraph Api
        logger.info( "Executing clustering..." );
        clusterer.cluster( graphId, bottom , top  );

        mergeEntities( linkedEntitySetId, ownablePropertyTypes, propertyTypesToPopulate );

        logger.info( "Linking job finished." );
        return linkedEntitySetId;
    }

    private void mergeEntities(
            UUID linkedEntitySetId,
            Set<UUID> ownablePropertyTypes,
            Set<UUID> propertyTypesToPopulate ) {
        Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets = new HashMap<>();

        // compute authorized property types for each of the linking entity sets, as well as the linked entity set
        // itself
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

        mergeVertices( linkedEntitySetId, syncId, authorizedPropertyTypesForEntitySets, propertyTypesToPopulate );

        mergeEdges( linkedEntitySetId, linkingSets, syncId );
    }

    private void mergeVertices(
            UUID linkedEntitySetId,
            UUID syncId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets,
            Set<UUID> propertyTypesToPopulate ) {
        logger.debug( "Linking: Merging vertices..." );

        Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataTypeForLinkedEntitySet = Maps.transformValues(
                authorizedPropertyTypesForEntitySets.get( linkedEntitySetId ), pt -> pt.getDatatype() );
        // EntityType.getKey returns an unmodifiable view of the underlying linked hash set, so the order is still
        // preserved, although
        List<UUID> keyProperties = new LinkedList<>( dms.getEntityTypeByEntitySetId( linkedEntitySetId ).getKey() );

        Iterable<Pair<UUID, Set<EntityKey>>> linkedEntityKeys = clgqs.getLinkedEntityKeys( linkedEntitySetId );
        for ( Pair<UUID, Set<EntityKey>> linkedKey : linkedEntityKeys ) {
            // compute merged entity
            SetMultimap<UUID, Object> mergedEntityDetails = computeMergedEntity( linkedKey.getValue(),
                    authorizedPropertyTypesForEntitySets,
                    propertyTypesToPopulate );
            String entityId = LoomUtil.generateDefaultEntityId( keyProperties, mergedEntityDetails );

            // create merged entity, in particular get back the entity key id for the new entity
            UUID mergedEntityKeyId;
            try {
                mergedEntityKeyId = dgm.createEntity( linkedEntitySetId,
                        syncId,
                        entityId,
                        mergedEntityDetails,
                        authorizedPropertiesWithDataTypeForLinkedEntitySet );

                // write to a lookup table from old entity key id to new, merged entity key id
                idService.getEntityKeyIds( linkedKey.getValue() ).values()
                        .forEach( oldId -> vms.saveToLookup( linkedEntitySetId, oldId, mergedEntityKeyId ) );

            } catch ( ExecutionException | InterruptedException e ) {
                logger.error( "Failed to create linked entity for linkedKey {} ", linkedKey );
            }
        }
    }

    private void mergeEdges( UUID linkedEntitySetId, Set<UUID> linkingSets, UUID syncId ) {
        logger.debug( "Linking: Merging edges..." );
        logger.debug( "Linking Sets: {}", linkingSets );
        Map<CommonColumns, Set<UUID>> edgeSelectionBySrcSet = ImmutableMap
                .of( CommonColumns.SRC_ENTITY_SET_ID, linkingSets );
        Map<CommonColumns, Set<UUID>> edgeSelectionByDstSet = ImmutableMap
                .of( CommonColumns.DST_ENTITY_SET_ID, linkingSets );
        Map<CommonColumns, Set<UUID>> edgeSelectionByEdgeSet = ImmutableMap
                .of( CommonColumns.EDGE_ENTITY_SET_ID, linkingSets );

        Stream.of( lm.getEdges( edgeSelectionBySrcSet ),
                lm.getEdges( edgeSelectionByDstSet ),
                lm.getEdges( edgeSelectionByEdgeSet ) )
                .reduce( Stream::concat )
                .orElseGet( Stream::empty )
                .map( edge -> mergeEdgeAsync( linkedEntitySetId, syncId, edge ) )
                .forEach( StreamUtil::getUninterruptibly );
    }

    private SetMultimap<UUID, Object> computeMergedEntity(
            Set<EntityKey> entityKeys,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets,
            Set<UUID> propertyTypesToPopulate ) {
        SetMultimap<UUID, Object> result = HashMultimap.create();

        entityKeys.stream()
                .map( key -> Pair.of( key.getEntitySetId(),
                        eds.asyncLoadEntity( key.getEntitySetId(),
                                key.getEntityId(),
                                key.getSyncId(),
                                authorizedPropertyTypesForEntitySets.get( key.getEntitySetId() ).keySet() ) ) )
                .map( rsfPair -> Pair.of( rsfPair.getKey(), StreamUtil.safeGet( rsfPair.getValue() ) ) )
                .map( rsPair -> RowAdapters.entityIndexedById( rsPair.getKey().toString(),
                        rsPair.getValue(),
                        authorizedPropertyTypesForEntitySets.get( rsPair.getKey() ),
                        propertyTypesToPopulate,
                        mapper ) )
                .forEach( result::putAll );
        return result;
    }

    private ListenableFuture mergeEdgeAsync( UUID linkedEntitySetId, UUID syncId, LoomEdge edge ) {
        UUID srcEntitySetId = edge.getSrcSetId();
        UUID srcSyncId = edge.getSrcSyncId();
        UUID dstEntitySetId = edge.getDstSetId();
        UUID dstSyncId = edge.getDstSyncId();
        UUID edgeEntitySetId = edge.getEdgeSetId();

        UUID srcId = edge.getKey().getSrcEntityKeyId();
        UUID dstId = edge.getKey().getDstEntityKeyId();
        UUID edgeId = edge.getKey().getEdgeEntityKeyId();

        UUID newSrcId = vms.getMergedId( linkedEntitySetId, srcId );
        if ( newSrcId != null ) {
            srcEntitySetId = linkedEntitySetId;
            srcSyncId = syncId;
            srcId = newSrcId;
        }
        UUID newDstId = vms.getMergedId( linkedEntitySetId, dstId );
        if ( newDstId != null ) {
            dstEntitySetId = linkedEntitySetId;
            dstSyncId = syncId;
            dstId = newDstId;
        }
        UUID newEdgeId = vms.getMergedId( linkedEntitySetId, edgeId );
        if ( newEdgeId != null ) {
            edgeEntitySetId = linkedEntitySetId;
            edgeId = newEdgeId;
        }

        lm.deleteEdge( edge.getKey() );
        return lm.addEdgeAsync( srcId,
                dms.getEntitySet( srcEntitySetId ).getEntityTypeId(),
                srcEntitySetId,
                srcSyncId,
                dstId,
                dms.getEntitySet( dstEntitySetId ).getEntityTypeId(),
                dstEntitySetId,
                dstSyncId,
                edgeId,
                dms.getEntitySet( edgeEntitySetId ).getEntityTypeId(),
                edgeEntitySetId );
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

    public Map<FullQualifiedName, String> getPropertyTypeIdIndexedByFqn( Set<Map<UUID, UUID>> linkingProperties ) {
        Map<FullQualifiedName, String> result = Maps.newHashMap();

        linkingProperties.stream().flatMap( m -> m.entrySet().stream() )
                .forEach( entry -> result.put( dms.getPropertyType( entry.getValue() ).getType(),
                        entry.getValue().toString() ) );

        return result;
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
