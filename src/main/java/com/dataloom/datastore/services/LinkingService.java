package com.dataloom.datastore.services;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.datasource.UUIDs.Syncs;
import com.dataloom.linking.Entity;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.linking.LinkingEdge;
import com.dataloom.linking.LinkingEntityKey;
import com.dataloom.linking.LinkingVertexKey;
import com.dataloom.linking.components.Blocker;
import com.dataloom.linking.components.Clusterer;
import com.dataloom.linking.components.Matcher;
import com.dataloom.linking.util.UnorderedPair;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;

public class LinkingService {

    private static final Logger          logger = LoggerFactory.getLogger( LinkingService.class );

    private HazelcastLinkingGraphs       linkingGraph;
    private Blocker                      blocker;
    private Matcher                      matcher;
    private Clusterer                    clusterer;

    private final DurableExecutorService executor;

    public LinkingService(
            HazelcastLinkingGraphs linkingGraph,
            Blocker blocker,
            Matcher matcher,
            Clusterer clusterer,
            HazelcastInstance hazelcast,
            EventBus eventBus ) {
        this.linkingGraph = linkingGraph;

        this.blocker = blocker;
        this.matcher = matcher;
        this.clusterer = clusterer;

        this.executor = hazelcast.getDurableExecutorService( "default" );

        eventBus.register( this );
    }

    public UUID link( UUID linkedEntitySetId, Set<Map<UUID, UUID>> linkingProperties ) {
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

        logger.info( "Executing matching..." );
        // Matching: check if pair score is already calculated from HazelcastGraph Api. If not, stream
        // through matcher to get a score.
        pairs
        //bad fix, will do for now.
                .filter( entityPair -> entityPair.getBackingCollection().size() == 2 )
                .filter( entityPair -> !edgeExists( graphId, entityPair ) )
                .forEach( entityPair -> {
                    LinkingEdge edge = fromUnorderedPair( graphId, entityPair );
                    double weight = matcher.dist( entityPair );
                    linkingGraph.addEdge( edge, weight );
                } );

        // Feed the scores (i.e. the edge set) into HazelcastGraph Api
        logger.info( "Executing clustering..." );
        clusterer.cluster( graphId );
        
        logger.info( "Linking job finished." );
        return linkedEntitySetId;
    }

    private LinkingEdge fromUnorderedPair( UUID graphId, UnorderedPair<Entity> p ) {
        List<LinkingEntityKey> keys = p.getBackingCollection().stream()
                .map( e -> new LinkingEntityKey( graphId, e.getKey() ) ).collect( Collectors.toList() );
        LinkingVertexKey u = linkingGraph.getLinkingVertextKey( keys.get( 0 ) );
        LinkingVertexKey v = linkingGraph.getLinkingVertextKey( keys.get( 1 ) );
        return new LinkingEdge( u, v );
    }

    private boolean edgeExists( UUID graphId, UnorderedPair<Entity> p ) {
        return linkingGraph.edgeExists( fromUnorderedPair( graphId, p ) );
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
