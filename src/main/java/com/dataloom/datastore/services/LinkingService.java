package com.dataloom.datastore.services;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.AbstractSecurableObjectResolveTypeService;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.datastore.linking.components.SimpleElasticSearchBlocker;
import com.dataloom.datastore.linking.controllers.SimpleHierarchicalClusterer;
import com.dataloom.datastore.linking.controllers.SimpleMatcher;
import com.dataloom.datastore.linking.controllers.SimpleMerger;
import com.dataloom.linking.Entity;
import com.dataloom.linking.components.Blocker;
import com.dataloom.linking.components.Clusterer;
import com.dataloom.linking.components.Matcher;
import com.dataloom.linking.components.Merger;
import com.dataloom.linking.util.UnorderedPair;
import com.dataloom.streams.StreamUtil;
import com.google.common.collect.Multimap;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.conductor.rpc.ConductorCall;
import com.kryptnostic.conductor.rpc.Lambdas;

public class LinkingService {

    private static final Logger          logger = LoggerFactory.getLogger( LinkingService.class );

    @Inject
    private EventBus                     eventBus;

    private final DurableExecutorService executor;

    public LinkingService( HazelcastInstance hazelcast ) {
        this.executor = hazelcast.getDurableExecutorService( "default" );
    }

    @PostConstruct
    public void initializeBus() {
        eventBus.register( this );
    }
    
    public UUID executeLinking( Map<UUID, UUID> entitySetsWithSyncIds, Multimap<UUID, UUID> linkingMap, Set<Map<UUID, UUID>> linkingProperties ){
        try {
            UUID linkedESId = executor.submit( ConductorCall
                    .wrap( Lambdas.executeLinking( entitySetsWithSyncIds, linkingMap, linkingProperties ) ) )
                    .get();
            return linkedESId;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Linking entity sets failed.", e );
        }
        return null;        
    }
}
