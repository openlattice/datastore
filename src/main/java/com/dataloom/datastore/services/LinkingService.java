package com.dataloom.datastore.services;

import com.codahale.metrics.annotation.Timed;
import com.dataloom.data.DataGraphManager;
import com.dataloom.data.DatasourceManager;
import com.dataloom.data.EntityDatastore;
import com.dataloom.data.EntityKeyIdService;
import com.dataloom.linking.aggregators.MergeEdgeAggregator;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.core.LoomGraphApi;
import com.dataloom.graph.edge.EdgeKey;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.graph.mapstores.PostgresEdgeMapstore;
import com.dataloom.linking.CassandraLinkingGraphsQueryService;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.linking.HazelcastListingService;
import com.dataloom.linking.HazelcastVertexMergingService;
import com.dataloom.linking.components.Blocker;
import com.dataloom.linking.components.Clusterer;
import com.dataloom.matching.DistributedMatcher;
import com.dataloom.merging.DistributedMerger;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.Predicates;
import com.kryptnostic.datastore.services.EdmManager;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinkingService {
    private static final Logger logger = LoggerFactory.getLogger( LinkingService.class );

    private final ObjectMapper                       mapper;
    private final HazelcastLinkingGraphs             linkingGraph;
    private final Blocker                            blocker;
    private final DistributedMatcher                 matcher;
    private final Clusterer                          clusterer;
    private final DistributedMerger                  merger;
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
            DistributedMatcher matcher,
            Clusterer clusterer,
            DistributedMerger merger,
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
        this.merger = merger;

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

    @Timed
    public UUID link(
            UUID linkedEntitySetId,
            Set<Map<UUID, UUID>> linkingProperties,
            Set<UUID> ownablePropertyTypes,
            Set<UUID> propertyTypesToPopulate ) {
        SetMultimap<UUID, UUID> linkIndexedByPropertyTypes = getLinkIndexedByPropertyTypes( linkingProperties );
        SetMultimap<UUID, UUID> linkIndexedByEntitySets = getLinkIndexedByEntitySets( linkingProperties );

        boolean isMatchingPerson = dms.getEntityTypeByEntitySetId( linkIndexedByEntitySets.keySet().iterator().next() )
                .getType().toString().equals( "general.person" );
        if ( !isMatchingPerson ) {
            throw new IllegalArgumentException(
                    "Data matching can only be performed on datasets of type general.person" );
        }

        // Warning: We assume that the restrictions on links are enforced/validated as specified in LinkingApi. In
        // particular, from now on we work on the assumption that only identical property types are linked on.
        initializeComponents( dsm.getCurrentSyncId( linkIndexedByEntitySets.keySet() ),
                linkIndexedByPropertyTypes,
                linkIndexedByEntitySets );

        UUID graphId = linkingGraph.getGraphIdFromEntitySetId( linkedEntitySetId );

        Stopwatch stopwatch = Stopwatch.createStarted();
        Stopwatch stopwatchAll = Stopwatch.createStarted();
        logger.info( "Executing matching..." );
        // Matching: check if pair score is already calculated from HazelcastGraph Api. If not, stream
        // through matcher to get a score.
        double minWeight = matcher.match( graphId );
        logger.info( "Matching finished, took: {}", stopwatch.elapsed( TimeUnit.SECONDS ) );
        logger.info( "Lightest: {}", minWeight );
        stopwatch.reset();
        stopwatch.start();

        // Feed the scores (i.e. the edge set) into HazelcastGraph Api
        logger.info( "Executing clustering..." );
        clusterer.cluster( graphId, minWeight );
        logger.info( "Clustering finished, took: {}", stopwatch.elapsed( TimeUnit.SECONDS ) );
        stopwatch.reset();
        stopwatch.start();
        merger.merge( graphId, ownablePropertyTypes, propertyTypesToPopulate );
        //mergeEntities( linkedEntitySetId, ownablePropertyTypes, propertyTypesToPopulate );
        logger.info( "Merging finished, took: {}", stopwatch.elapsed( TimeUnit.SECONDS ) );

        logger.info( "Linking job finished, took: {}", stopwatchAll.elapsed( TimeUnit.SECONDS ) );
        return linkedEntitySetId;
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
