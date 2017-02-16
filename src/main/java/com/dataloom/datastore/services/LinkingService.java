package com.dataloom.datastore.services;

import com.dataloom.datasource.UUIDs.Syncs;
import com.dataloom.graph.GraphUtil;
import com.dataloom.linking.*;
import com.dataloom.linking.components.Blocker;
import com.dataloom.linking.components.Matcher;
import com.dataloom.linking.util.UnorderedPair;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.conductor.rpc.ConductorCall;
import com.kryptnostic.conductor.rpc.Lambdas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LinkingService {

    private static final Logger          logger = LoggerFactory.getLogger( LinkingService.class );

    private Blocker                      blocker;
    private Matcher                      matcher;
    private HazelcastLinkingGraphs       linkingGraph;

    private final DurableExecutorService executor;

    public LinkingService(
            Blocker blocker,
            Matcher matcher,
            HazelcastLinkingGraphs linkingGraph,
            HazelcastInstance hazelcast , EventBus eventBus ) {
        this.blocker = blocker;
        this.matcher = matcher;
        this.linkingGraph = linkingGraph;

        this.executor = hazelcast.getDurableExecutorService( "default" );
    }


    public UUID link( UUID linkedEntitySetId, Set<Map<UUID, UUID>> linkingProperties ) {
        SetMultimap<UUID, UUID> linkIndexedByPropertyTypes = getLinkIndexedByPropertyTypes( linkingProperties );
        SetMultimap<UUID, UUID> linkIndexedByEntitySets = getLinkIndexedByEntitySets( linkingProperties );

        // TODO may be deprecated, depending on whether syncIds is required to do reads.
        Map<UUID, UUID> entitySetsWithSyncIds = linkIndexedByEntitySets.keySet().stream()
                .collect( Collectors.toMap( esId -> esId, esId -> Syncs.BASE.getSyncId() ) );

        // Warning: We assume that the restrictions on links are enforced/validated as specified in LinkingApi. In particular, from now on we work on the assumption that only identical property types are linked on.
        initialize( entitySetsWithSyncIds, linkIndexedByPropertyTypes, linkIndexedByEntitySets );

        // Blocking: For each row in the entity sets turned dataframes, fire off query to elasticsearch
        Stream<UnorderedPair<Entity>> pairs = blocker.block();

        // Matching: check if pair score is already calculated, presumably from HazelcastGraph Api. If not, stream
        // through matcher to get a score.
        pairs
                .filter( entityPair -> edgeExists( linkedEntitySetId, entityPair) )
                .forEach( entityPair -> {
                    LinkingEdge edge = fromUnorderedPair( linkedEntitySetId , entityPair );
                    double weight = matcher.score( entityPair );
                    linkingGraph.addEdge( edge, weight );
                } );

        // Feed the scores (i.e. the edge set) into HazelcastGraph Api

        /**
         * Got here right now.
         */

        try {
            executor.submit( ConductorCall
                    .wrap( Lambdas.clustering( linkedEntitySetId ) ) )
                    .get();
            return linkedEntitySetId;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Linking entity sets failed.", e );
        }
        return null;
    }

    private LinkingEdge fromUnorderedPair( UUID entitySetId ,UnorderedPair<Entity> p ) {
        List<LinkingEntityKey> keys = p.getBackingCollection().stream()
                .map( e -> new LinkingEntityKey( entitySetId, e.getKey() ) ).collect( Collectors.toList() );
        LinkingVertexKey u = linkingGraph.getLinkingVertextKey( keys.get( 0 ) );
        LinkingVertexKey v = linkingGraph.getLinkingVertextKey( keys.get( 1 ) );
        return new LinkingEdge( u, v );
    }

    private boolean edgeExists( UUID entitySetId, UnorderedPair<Entity> p ) {
        return linkingGraph.edgeExists( fromUnorderedPair( entitySetId, p ) );
    }

    private void initialize(
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

    public static Set<UUID> getLinkingSets( Set<Map<UUID, UUID>> linkingProperties ){
        return getLinkIndexedByEntitySets( linkingProperties ).keySet();
    }

}
