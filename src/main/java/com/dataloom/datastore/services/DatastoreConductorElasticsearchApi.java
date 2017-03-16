package com.dataloom.datastore.services;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.linking.Entity;
import com.dataloom.organization.Organization;
import com.dataloom.search.requests.SearchResult;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.conductor.rpc.AdvancedSearchEntitySetDataLambda;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchCall;
import com.kryptnostic.conductor.rpc.ElasticsearchLambdas;
import com.kryptnostic.conductor.rpc.EntityDataLambdas;
import com.kryptnostic.conductor.rpc.SearchEntitySetDataAcrossIndicesLambda;
import com.kryptnostic.conductor.rpc.SearchEntitySetDataLambda;

public class DatastoreConductorElasticsearchApi implements ConductorElasticsearchApi {

    private static final Logger          logger = LoggerFactory.getLogger( DatastoreConductorElasticsearchApi.class );

    private final DurableExecutorService executor;

    public DatastoreConductorElasticsearchApi( HazelcastInstance hazelcast ) {
        this.executor = hazelcast.getDurableExecutorService( "default" );
    }

    @Override
    public boolean saveEntitySetToElasticsearch(
            EntitySet entitySet,
            List<PropertyType> propertyTypes,
            Principal principal ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.submitEntitySetToElasticsearch(
                            entitySet,
                            propertyTypes,
                            principal ) ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save entity set to elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean deleteEntitySet( UUID entitySetId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteEntitySet( entitySetId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete entity set from elasticsearch" );
            return false;
        }
    }

    @Override
    public SearchResult executeEntitySetMetadataSearch(
            Optional<String> optionalSearchTerm,
            Optional<UUID> optionalEntityType,
            Optional<Set<UUID>> optionalPropertyTypes,
            Set<Principal> principals,
            int start,
            int maxHits ) {
        try {
            SearchResult searchResult = executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.executeEntitySetMetadataQuery( optionalSearchTerm,
                            optionalEntityType,
                            optionalPropertyTypes,
                            start,
                            maxHits ) ) )
                    .get();
            return searchResult;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to to perofrm keyword search.", e );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public boolean updateEntitySetPermissions( UUID entitySetId, Principal principal, Set<Permission> permissions ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.updateEntitySetPermissions( entitySetId, principal, permissions ) ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to update entity set permissions in elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean updateEntitySetMetadata( EntitySet entitySet ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.updateEntitySetMetadata( entitySet ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to update entity set metadata in elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean updatePropertyTypesInEntitySet( UUID entitySetId, List<PropertyType> newPropertyTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.updatePropertyTypesInEntitySet( entitySetId,
                            newPropertyTypes ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to update property types in entity set in elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean createOrganization( Organization organization, Principal principal ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.createOrganization( organization, principal ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to create organization in elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean updateOrganizationPermissions(
            UUID organizationId,
            Principal principal,
            Set<Permission> permissions ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.updateOrganizationPermissions( organizationId,
                            principal,
                            permissions ) ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to update organization permissions" );
            return false;
        }
    }

    @Override
    public boolean deleteOrganization( UUID organizationId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteOrganization( organizationId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete organization from elasticsearch" );
            return false;
        }
    }

    @Override
    public SearchResult executeOrganizationSearch(
            String searchTerm,
            Set<Principal> principals,
            int start,
            int maxHits ) {
        try {
            SearchResult searchResult = executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.executeOrganizationKeywordSearch( searchTerm,
                            start,
                            maxHits ) ) )
                    .get();
            return searchResult;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to to perform keyword search.", e );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public boolean updateOrganization( UUID id, Optional<String> optionalTitle, Optional<String> optionalDescription ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.updateOrganization( id,
                            optionalTitle,
                            optionalDescription ) ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to update organization in elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean createEntityData( UUID entitySetId, String entityId, Map<UUID, Object> propertyValues ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    new EntityDataLambdas( entitySetId, entityId, propertyValues ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save entity data to elasticsearch" );
            return false;
        }
    }

    @Override
    public SearchResult executeEntitySetDataSearch(
            UUID entitySetId,
            String searchTerm,
            int start,
            int maxHits,
            Set<UUID> authorizedPropertyTypes ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    new SearchEntitySetDataLambda(
                            entitySetId,
                            searchTerm,
                            start,
                            maxHits,
                            authorizedPropertyTypes ) ) )
                    .get();
            return queryResults;

        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute entity set data search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public List<Entity> executeEntitySetDataSearchAcrossIndices(
            Set<UUID> entitySetIds,
            Map<UUID, Set<String>> fieldSearches,
            int size,
            boolean explain ) {
        try {
            List<Entity> queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    new SearchEntitySetDataAcrossIndicesLambda( entitySetIds, fieldSearches, size, explain ) ) ).get();
            return queryResults;

        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Failed to execute search for entity set data search across indices: " + fieldSearches );
            return Lists.newArrayList();
        }
    }

    @Override
    public SearchResult executeAdvancedEntitySetDataSearch(
            UUID entitySetId,
            Map<UUID, String> searches,
            int start,
            int maxHits,
            Set<UUID> authorizedPropertyTypes ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    new AdvancedSearchEntitySetDataLambda(
                            entitySetId,
                            searches,
                            start,
                            maxHits,
                            authorizedPropertyTypes ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute entity set data search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

}
