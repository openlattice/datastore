package com.dataloom.datastore.services;

import com.codahale.metrics.annotation.Timed;
import com.dataloom.data.DatasourceManager;
import com.dataloom.edm.type.EntityType;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.linking.components.Clusterer;
import com.dataloom.matching.DistributedMatcher;
import com.dataloom.merging.DistributedMerger;
import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.kryptnostic.datastore.services.EdmManager;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class LinkingService {
    private static final FullQualifiedName BASE_PERSON_TYPE = new FullQualifiedName( "general.person" );
    private static final Logger            logger           = LoggerFactory.getLogger( LinkingService.class );

    private final HazelcastLinkingGraphs             linkingGraph;
    private final DistributedMatcher                 matcher;
    private final Clusterer                          clusterer;
    private final DistributedMerger                  merger;
    private final EdmManager                         dms;
    private final DatasourceManager                  dsm;

    public LinkingService(
            HazelcastLinkingGraphs linkingGraph,
            DistributedMatcher matcher,
            Clusterer clusterer,
            DistributedMerger merger,
            EventBus eventBus,
            EdmManager dms,
            DatasourceManager dsm  ) {
        this.linkingGraph = linkingGraph;
        this.matcher = matcher;
        this.clusterer = clusterer;
        this.merger = merger;
        this.dms = dms;
        this.dsm = dsm;

        eventBus.register( this );
    }

    private void validateMatchingTypes( Set<UUID> entitySetIds ) {
        entitySetIds.forEach( entitySetId -> {
            EntityType et = dms.getEntityTypeByEntitySetId( entitySetId );
            while ( !et.getType().equals( BASE_PERSON_TYPE ) ) {
                if ( !et.getBaseType().isPresent() ) {
                    throw new IllegalArgumentException(
                            "Data matching can only be performed on datasets of type general.person" );
                }
                et = dms.getEntityType( et.getBaseType().get() );
            }
        } );
    }

    @Timed
    public UUID link(
            UUID linkedEntitySetId,
            Set<Map<UUID, UUID>> linkingProperties,
            Set<UUID> ownablePropertyTypes,
            Set<UUID> propertyTypesToPopulate ) {
        SetMultimap<UUID, UUID> linkIndexedByPropertyTypes = getLinkIndexedByPropertyTypes( linkingProperties );
        SetMultimap<UUID, UUID> linkIndexedByEntitySets = getLinkIndexedByEntitySets( linkingProperties );

        validateMatchingTypes( linkIndexedByEntitySets.keySet() );

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
