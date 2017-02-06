package com.dataloom.datastore.services;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.AclData;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.events.AclUpdateEvent;
import com.dataloom.edm.events.EntitySetCreatedEvent;
import com.dataloom.edm.events.EntitySetDeletedEvent;
import com.dataloom.organizations.events.OrganizationCreatedEvent;
import com.dataloom.organizations.events.OrganizationDeletedEvent;
import com.dataloom.organizations.events.OrganizationUpdatedEvent;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.conductor.rpc.ConductorCall;
import com.kryptnostic.conductor.rpc.Lambdas;

public class SearchService {
    private static final Logger          logger = LoggerFactory.getLogger( SearchService.class );

    @Inject
    private EventBus                     eventBus;

    @Inject
    private AuthorizationManager         authorizations;

    private final DurableExecutorService executor;

    public SearchService( HazelcastInstance hazelcast ) {
        this.executor = hazelcast.getDurableExecutorService( "default" );
    }

    @PostConstruct
    public void initializeBus() {
        eventBus.register( this );
    }

    public List<Map<String, Object>> executeEntitySetKeywordSearchQuery(
            Optional<String> optionalQuery,
            Optional<UUID> optionalEntityType,
            Optional<Set<UUID>> optionalPropertyTypes ) {
        try {
            List<Map<String, Object>> queryResults = executor.submit( ConductorCall
                    .wrap( Lambdas.executeElasticsearchMetadataQuery( optionalQuery,
                            optionalEntityType,
                            optionalPropertyTypes ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to to perofrm keyword search.", e );
        }
        return null;
    }

    public void updateEntitySetPermissions( List<UUID> aclKeys, Principal principal, Set<Permission> permissions ) {
        aclKeys.forEach( aclKey -> {
            executor.submit( ConductorCall
                    .wrap( Lambdas.updateEntitySetPermissions( aclKey, principal, permissions ) ) );
        } );
    }

    @Subscribe
    public void onAclUpdate( AclUpdateEvent event ) {
        event.getPrincipals().forEach( principal -> updateEntitySetPermissions(
                event.getAclKeys(),
                principal,
                authorizations.getSecurableObjectPermissions( event.getAclKeys(), Sets.newHashSet( principal ) ) ) );
    }

    @Subscribe
    public void createEntitySet( EntitySetCreatedEvent event ) {
        executor.submit( ConductorCall
                .wrap( Lambdas.submitEntitySetToElasticsearch(
                        event.getEntitySet(),
                        event.getPropertyTypes(),
                        event.getPrincipal() ) ) );
    }

    @Subscribe
    public void deleteEntitySet( EntitySetDeletedEvent event ) {
        executor.submit( ConductorCall
                .wrap( Lambdas.deleteEntitySet( event.getEntitySetId() ) ) );
    }
    
    @Subscribe
    public void createOrganization( OrganizationCreatedEvent event ) {
        executor.submit( ConductorCall
                .wrap( Lambdas.createOrganization( event.getOrganization(), event.getPrincipal() ) ) );
    }
    
    public List<Map<String, Object>> executeOrganizationKeywordSearch( String searchTerm ) {
        try {
            List<Map<String, Object>> queryResults = executor.submit( ConductorCall
                    .wrap( Lambdas.executeOrganizationKeywordSearch( searchTerm ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to to perofrm keyword search.", e );
        }
        return null;
    }
    
    @Subscribe
    public void updateOrganization( OrganizationUpdatedEvent event ) {
        executor.submit( ConductorCall
                .wrap( Lambdas.updateOrganization( event.getId(), event.getOptionalTitle(), event.getOptionalDescription() ) ) );
    }
    
    @Subscribe
    public void deleteOrganization( OrganizationDeletedEvent event ) {
        executor.submit( ConductorCall
                .wrap( Lambdas.deleteOrganization( event.getOrganizationId() ) ) );
    }

}
