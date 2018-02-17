package com.dataloom.datastore.services;

import com.openlattice.apps.App;
import com.openlattice.apps.AppType;
import com.openlattice.data.EntityKey;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.organization.Organization;
import com.openlattice.search.requests.SearchDetails;
import com.openlattice.search.requests.SearchResult;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.conductor.rpc.*;
import com.openlattice.authorization.AclKey;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class DatastoreConductorElasticsearchApi implements ConductorElasticsearchApi {

    private static final Logger logger = LoggerFactory.getLogger( DatastoreConductorElasticsearchApi.class );

    private final DurableExecutorService executor;

    public DatastoreConductorElasticsearchApi( HazelcastInstance hazelcast ) {
        this.executor = hazelcast.getDurableExecutorService( "default" );
    }

    @Override
    public boolean saveEntitySetToElasticsearch( EntitySet entitySet, List<PropertyType> propertyTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.submitEntitySetToElasticsearch( entitySet, propertyTypes ) ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save entity set to elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean createSecurableObjectIndex( UUID entitySetId, UUID syncId, List<PropertyType> propertyTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.createSecurableObjectIndex(
                            entitySetId,
                            syncId,
                            propertyTypes ) ) )
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
    public boolean deleteEntitySetForSyncId( UUID entitySetId, UUID syncId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteEntitySetForSyncId( entitySetId, syncId ) ) ).get();
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
            Set<AclKey> authorizedAclKeys,
            int start,
            int maxHits ) {
        try {
            SearchResult searchResult = executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.executeEntitySetMetadataQuery( optionalSearchTerm,
                            optionalEntityType,
                            optionalPropertyTypes,
                            authorizedAclKeys,
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
                            newPropertyTypes ) ) )
                    .get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to update property types in entity set in elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean createOrganization( Organization organization ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.createOrganization( organization ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to create organization in elasticsearch" );
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
            Set<AclKey> authorizedOrganizationIds,
            int start,
            int maxHits ) {
        try {
            SearchResult searchResult = executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.executeOrganizationKeywordSearch( searchTerm,
                            authorizedOrganizationIds,
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
    public boolean createEntityData(
            UUID entitySetId,
            UUID syncId,
            String entityId,
            SetMultimap<UUID, Object> propertyValues ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    new EntityDataLambdas( entitySetId, syncId, entityId, propertyValues, false ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save entity data to elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean updateEntityData(
            UUID entitySetId,
            UUID syncId,
            String entityId,
            SetMultimap<UUID, Object> propertyValues ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    new EntityDataLambdas( entitySetId, syncId, entityId, propertyValues, true ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to update entity data in elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean deleteEntityData( UUID entitySetId, UUID syncId, String entityId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteEntityData( entitySetId, syncId, entityId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete entity data from elasticsearch" );
            return false;
        }
    }

    @Override
    public SearchResult executeEntitySetDataSearch(
            UUID entitySetId,
            UUID syncId,
            String searchTerm,
            int start,
            int maxHits,
            Set<UUID> authorizedPropertyTypes ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    new SearchEntitySetDataLambda(
                            entitySetId,
                            syncId,
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
    public List<EntityKey> executeEntitySetDataSearchAcrossIndices(
            Map<UUID, UUID> entitySetAndSyncIds,
            Map<UUID, DelegatedStringSet> fieldSearches,
            int size,
            boolean explain ) {
        try {
            List<EntityKey> queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    new SearchEntitySetDataAcrossIndicesLambda( entitySetAndSyncIds, fieldSearches, size, explain ) ) )
                    .get();
            return queryResults;

        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Failed to execute search for entity set data search across indices: " + fieldSearches );
            return Lists.newArrayList();
        }
    }

    @Override
    public SearchResult executeAdvancedEntitySetDataSearch(
            UUID entitySetId,
            UUID syncId,
            List<SearchDetails> searches,
            int start,
            int maxHits,
            Set<UUID> authorizedPropertyTypes ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    new AdvancedSearchEntitySetDataLambda(
                            entitySetId,
                            syncId,
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

    @Override
    public boolean saveEntityTypeToElasticsearch( EntityType entityType ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.saveEntityTypeToElasticsearch( entityType ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save entity type to elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean saveAssociationTypeToElasticsearch( AssociationType associationType ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.saveAssociationTypeToElasticsearch( associationType ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save association type to elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean savePropertyTypeToElasticsearch( PropertyType propertyType ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.savePropertyTypeToElasticsearch( propertyType ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save property type to elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean deleteEntityType( UUID entityTypeId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteEntityType( entityTypeId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete entity type from elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean deleteAssociationType( UUID associationTypeId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteAssociationType( associationTypeId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete association type from elasticsearch" );
            return false;
        }
    }

    @Override
    public boolean deletePropertyType( UUID propertyTypeId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deletePropertyType( propertyTypeId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete property type from elasticsearch" );
            return false;
        }
    }

    @Override
    public SearchResult executeEntityTypeSearch( String searchTerm, int start, int maxHits ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.executeEntityTypeSearch(
                            searchTerm,
                            start,
                            maxHits ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute entity type search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public SearchResult executeAssociationTypeSearch( String searchTerm, int start, int maxHits ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.executeAssociationTypeSearch(
                            searchTerm,
                            start,
                            maxHits ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute association type search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public SearchResult executePropertyTypeSearch( String searchTerm, int start, int maxHits ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.executePropertyTypeSearch(
                            searchTerm,
                            start,
                            maxHits ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute property type search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public SearchResult executeFQNEntityTypeSearch( String namespace, String name, int start, int maxHits ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.executeFQNEntityTypeSearch(
                            namespace,
                            name,
                            start,
                            maxHits ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute property type search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public SearchResult executeFQNPropertyTypeSearch( String namespace, String name, int start, int maxHits ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.executeFQNPropertyTypeSearch(
                            namespace,
                            name,
                            start,
                            maxHits ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute property type search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override
    public boolean clearAllData() {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.clearAllData() ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete all data" );
            return false;
        }
    }

    @Override
    public double getModelScore( double[][] features ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.getModelScore( features ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to get model score" );
            return Double.MAX_VALUE;
        }
    }

    @Override
    public boolean triggerPropertyTypeIndex( List<PropertyType> propertyTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.triggerPropertyTypeIndex( propertyTypes ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to trigger property type re-index" );
            return false;
        }
    }

    @Override
    public boolean triggerEntityTypeIndex( List<EntityType> entityTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.triggerEntityTypeIndex( entityTypes ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to trigger entity type re-index" );
            return false;
        }
    }

    @Override
    public boolean triggerAssociationTypeIndex( List<AssociationType> associationTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.triggerAssociationTypeIndex( associationTypes ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to trigger association type re-index" );
            return false;
        }
    }

    @Override
    public boolean triggerEntitySetIndex(
            Map<EntitySet, Set<UUID>> entitySets,
            Map<UUID, PropertyType> propertyTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.triggerEntitySetIndex( entitySets, propertyTypes ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to trigger entity set re-index" );
            return false;
        }
    }

    @Override public boolean triggerAppIndex( List<App> apps ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.triggerAppIndex( apps ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to trigger app re-index" );
            return false;
        }
    }

    @Override public boolean triggerAppTypeIndex( List<AppType> appTypes ) {
        try {
            return executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.triggerAppTypeIndex( appTypes ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to trigger app type re-index" );
            return false;
        }
    }

    @Override public boolean saveAppToElasticsearch( App app ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.saveAppToElasticsearch( app ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save app to elasticsearch" );
            return false;
        }
    }

    @Override public boolean deleteApp( UUID appId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteApp( appId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete app from elasticsearch" );
            return false;
        }
    }

    @Override public SearchResult executeAppSearch( String searchTerm, int start, int maxHits ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.executeAppSearch(
                            searchTerm,
                            start,
                            maxHits ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute app search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

    @Override public boolean saveAppTypeToElasticsearch( AppType appType ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.saveAppTypeToElasticsearch( appType ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to save app type to elasticsearch" );
            return false;
        }
    }

    @Override public boolean deleteAppType( UUID appTypeId ) {
        try {
            return executor.submit( ConductorElasticsearchCall
                    .wrap( ElasticsearchLambdas.deleteAppType( appTypeId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to delete app type from elasticsearch" );
            return false;
        }
    }

    @Override public SearchResult executeAppTypeSearch( String searchTerm, int start, int maxHits ) {
        try {
            SearchResult queryResults = executor.submit( ConductorElasticsearchCall.wrap(
                    ElasticsearchLambdas.executeAppTypeSearch(
                            searchTerm,
                            start,
                            maxHits ) ) )
                    .get();
            return queryResults;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute app type search" );
            return new SearchResult( 0, Lists.newArrayList() );
        }
    }

}
