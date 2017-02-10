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

import com.dataloom.authorization.AbstractSecurableObjectResolveTypeService;
import com.dataloom.authorization.AclData;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.authorization.events.AclUpdateEvent;
import com.dataloom.data.events.EntityDataCreatedEvent;
import com.dataloom.edm.events.EntitySetCreatedEvent;
import com.dataloom.edm.events.EntitySetDeletedEvent;
import com.dataloom.organizations.events.OrganizationCreatedEvent;
import com.dataloom.organizations.events.OrganizationDeletedEvent;
import com.dataloom.organizations.events.OrganizationUpdatedEvent;
import com.dataloom.edm.events.EntitySetMetadataUpdatedEvent;
import com.dataloom.edm.events.PropertyTypesInEntitySetUpdatedEvent;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.conductor.rpc.ConductorCall;
import com.kryptnostic.conductor.rpc.EntityDataLambdas;
import com.kryptnostic.conductor.rpc.Lambdas;
import com.kryptnostic.conductor.rpc.SearchEntitySetDataLambda;

public class SearchService {
    private static final Logger          logger = LoggerFactory.getLogger( SearchService.class );

    @Inject
    private EventBus                     eventBus;

    @Inject
    private AuthorizationManager         authorizations;
    
    @Inject
    private AbstractSecurableObjectResolveTypeService securableObjectTypes;

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
    
    public void updateOrganizationPermissions( List<UUID> aclKeys, Principal principal, Set<Permission> permissions ) {
        aclKeys.forEach( aclKey -> {
            executor.submit( ConductorCall
                    .wrap( Lambdas.updateOrganizationPermissions( aclKey, principal, permissions ) ) );
        } );
    }

    @Subscribe
    public void onAclUpdate( AclUpdateEvent event ) {
        SecurableObjectType type = securableObjectTypes.getSecurableObjectType( event.getAclKeys() );
        if ( type == SecurableObjectType.EntitySet ) {
            event.getPrincipals().forEach( principal -> updateEntitySetPermissions(
                    event.getAclKeys(),
                    principal,
                    authorizations.getSecurableObjectPermissions( event.getAclKeys(), Sets.newHashSet( principal ) ) ) );
        } else if ( type == SecurableObjectType.Organization ) {
            event.getPrincipals().forEach( principal -> updateOrganizationPermissions(
                    event.getAclKeys(),
                    principal,
                    authorizations.getSecurableObjectPermissions( event.getAclKeys(), Sets.newHashSet( principal ) ) ) );
        }
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
            return Lists.newArrayList();
        }
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
    
    @Subscribe
    public void createEntityData( EntityDataCreatedEvent event ) {
        executor.submit( ConductorCall.wrap( new EntityDataLambdas( event.getEntitySetId(), event.getEntityId(), event.getPropertyValues() ) ) );
    }
    
    public List<Map<String, Object>> executeEntitySetDataSearch( UUID entitySetId, String searchTerm, Set<UUID> authorizedProperties ) {
        List<Map<String, Object>> queryResults;
        try {
            queryResults = executor.submit( ConductorCall.wrap( 
                    new SearchEntitySetDataLambda( entitySetId, searchTerm, authorizedProperties ) ) ).get();
            return queryResults;

        } catch ( InterruptedException | ExecutionException e ) {
            e.printStackTrace();
            return Lists.newArrayList();
        }
    }

    @Subscribe
    public void updateEntitySetMetadata( EntitySetMetadataUpdatedEvent event ) {
        executor.submit( ConductorCall
                .wrap( Lambdas.updateEntitySetMetadata( event.getEntitySet() ) ) );
    }
    
    @Subscribe
    public void updatePropertyTypesInEntitySet( PropertyTypesInEntitySetUpdatedEvent event ) {
        executor.submit( ConductorCall
                .wrap( Lambdas.updatePropertyTypesInEntitySet( event.getEntitySetId(), event.getNewPropertyTypes() ) ) );
    }

}
