/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.datastore.services;

import com.codahale.metrics.annotation.Timed;
import com.dataloom.apps.App;
import com.dataloom.apps.AppType;
import com.dataloom.authorization.*;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.data.DatasourceManager;
import com.dataloom.data.EntityKey;
import com.dataloom.data.EntityKeyIdService;
import com.dataloom.data.events.EntityDataCreatedEvent;
import com.dataloom.data.events.EntityDataDeletedEvent;
import com.dataloom.data.requests.NeighborEntityDetails;
import com.dataloom.data.storage.CassandraEntityDatastore;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.events.*;
import com.dataloom.edm.type.AssociationType;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.core.LoomGraphApi;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.organizations.events.OrganizationCreatedEvent;
import com.dataloom.organizations.events.OrganizationDeletedEvent;
import com.dataloom.organizations.events.OrganizationUpdatedEvent;
import com.dataloom.search.requests.*;
import com.dataloom.streams.StreamUtil;
import com.dataloom.sync.events.CurrentSyncUpdatedEvent;
import com.dataloom.sync.events.SyncIdCreatedEvent;
import com.google.common.base.Optional;
import com.google.common.collect.*;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.kryptnostic.datastore.services.EdmManager;
import com.openlattice.authorization.AclKey;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

public class SearchService {
    private static final Logger logger = LoggerFactory.getLogger( SearchService.class );

    @Inject
    private EventBus eventBus;

    @Inject
    private AuthorizationManager authorizations;

    @Inject
    private AbstractSecurableObjectResolveTypeService securableObjectTypes;

    @Inject
    private DatastoreConductorElasticsearchApi elasticsearchApi;

    @Inject
    private DatasourceManager datasourceManager;

    @Inject
    private EdmManager dataModelService;

    @Inject
    private LoomGraphApi graphApi;

    @Inject
    private CassandraEntityDatastore dataManager;

    @Inject
    private EdmAuthorizationHelper authzHelper;

    @Inject
    private EntityKeyIdService entityKeyService;

    @PostConstruct
    public void initializeBus() {
        eventBus.register( this );
    }

    public SearchResult executeEntitySetKeywordSearchQuery(
            Optional<String> optionalQuery,
            Optional<UUID> optionalEntityType,
            Optional<Set<UUID>> optionalPropertyTypes,
            int start,
            int maxHits ) {

        Set<AclKey> authorizedEntitySetIds = authorizations
                .getAuthorizedObjectsOfType( Principals.getCurrentPrincipals(),
                        SecurableObjectType.EntitySet,
                        EnumSet.of( Permission.READ ) ).collect( Collectors.toSet() );
        if ( authorizedEntitySetIds.size() == 0 )
            return new SearchResult( 0, Lists.newArrayList() );

        return elasticsearchApi.executeEntitySetMetadataSearch( optionalQuery,
                optionalEntityType,
                optionalPropertyTypes,
                authorizedEntitySetIds,
                start,
                maxHits );
    }

    @Subscribe
    public void createEntitySet( EntitySetCreatedEvent event ) {
        elasticsearchApi.saveEntitySetToElasticsearch( event.getEntitySet(), event.getPropertyTypes() );
    }

    @Subscribe
    public void createSecurableObjectIndexForSyncId( SyncIdCreatedEvent event ) {
        UUID entitySetId = event.getEntitySetId();
        List<PropertyType> propertyTypes = Lists.newArrayList( dataModelService.getPropertyTypes(
                dataModelService.getEntityType( dataModelService.getEntitySet( entitySetId ).getEntityTypeId() )
                        .getProperties() ) );
        elasticsearchApi.createSecurableObjectIndex( entitySetId,
                event.getSyncId(),
                propertyTypes );
    }

    @Subscribe
    public void deleteEntitySet( EntitySetDeletedEvent event ) {
        elasticsearchApi.deleteEntitySet( event.getEntitySetId() );
    }

    @Subscribe
    public void deleteIndicesBeforeCurrentSync( CurrentSyncUpdatedEvent event ) {
        UUID entitySetId = event.getEntitySetId();
        datasourceManager.getAllPreviousSyncIds( entitySetId, event.getCurrentSyncId() ).forEach( syncId -> {
            elasticsearchApi.deleteEntitySetForSyncId( entitySetId, syncId );
        } );
    }

    @Subscribe
    public void createOrganization( OrganizationCreatedEvent event ) {
        elasticsearchApi.createOrganization( event.getOrganization() );
    }

    public SearchResult executeOrganizationKeywordSearch( SearchTerm searchTerm ) {
        Set<AclKey> authorizedOrganizationIds = authorizations
                .getAuthorizedObjectsOfType( Principals.getCurrentPrincipals(),
                        SecurableObjectType.Organization,
                        EnumSet.of( Permission.READ ) ).collect( Collectors.toSet() );
        if ( authorizedOrganizationIds.size() == 0 )
            return new SearchResult( 0, Lists.newArrayList() );

        return elasticsearchApi.executeOrganizationSearch( searchTerm.getSearchTerm(),
                authorizedOrganizationIds,
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @Subscribe
    public void updateOrganization( OrganizationUpdatedEvent event ) {
        elasticsearchApi.updateOrganization( event.getId(),
                event.getOptionalTitle(),
                event.getOptionalDescription() );
    }

    @Subscribe
    public void deleteOrganization( OrganizationDeletedEvent event ) {
        elasticsearchApi.deleteOrganization( event.getOrganizationId() );
    }

    @Subscribe
    public void createEntityData( EntityDataCreatedEvent event ) {
        UUID entitySetId = event.getEntitySetId();
        UUID syncId = ( event.getOptionalSyncId().isPresent() ) ? event.getOptionalSyncId().get()
                : datasourceManager.getCurrentSyncId( event.getEntitySetId() );
        String entityId = event.getEntityId();
        SetMultimap<UUID, Object> entity = event.getPropertyValues();
        if ( event.getShouldUpdate() )
            elasticsearchApi.updateEntityData( entitySetId, syncId, entityId, entity );
        else
            elasticsearchApi.createEntityData( entitySetId, syncId, entityId, entity );
    }

    @Subscribe
    public void deleteEntityData( EntityDataDeletedEvent event ) {
        Iterable<UUID> syncIds = ( event.getSyncId().isPresent() ) ? Lists.newArrayList( event.getSyncId().get() )
                : datasourceManager.getAllSyncIds( event.getEntitySetId() );
        syncIds.forEach(
                syncId -> elasticsearchApi.deleteEntityData( event.getEntitySetId(), syncId, event.getEntityId() ) );
    }

    @Timed
    public DataSearchResult executeEntitySetDataSearch(
            UUID entitySetId,
            SearchTerm searchTerm,
            Set<UUID> authorizedProperties ) {
        UUID syncId = datasourceManager.getCurrentSyncId( entitySetId );
        SearchResult result = elasticsearchApi.executeEntitySetDataSearch( entitySetId,
                syncId,
                searchTerm.getSearchTerm(),
                searchTerm.getStart(),
                searchTerm.getMaxHits(),
                authorizedProperties );
        Map<UUID, PropertyType> authorizedPropertyTypes = dataModelService
                .getPropertyTypesAsMap( authorizedProperties );

        List<SetMultimap<Object, Object>> results = getResults( entitySetId, syncId, result, authorizedPropertyTypes );

        return new DataSearchResult( result.getNumHits(), results );
    }

    @Timed
    public long getEntitySetSize( UUID entitySetId ) {
        UUID syncId = datasourceManager.getCurrentSyncId( entitySetId );
        Set<UUID> properties = Sets.newHashSet( dataModelService.getEntityTypeByEntitySetId( entitySetId ).getProperties() );
        return elasticsearchApi.executeEntitySetDataSearch( entitySetId, syncId, "*", 0, 0, properties ).getNumHits();
    }

    @Subscribe
    public void updateEntitySetMetadata( EntitySetMetadataUpdatedEvent event ) {
        elasticsearchApi.updateEntitySetMetadata( event.getEntitySet() );
    }

    @Subscribe
    public void updatePropertyTypesInEntitySet( PropertyTypesInEntitySetUpdatedEvent event ) {
        elasticsearchApi.updatePropertyTypesInEntitySet( event.getEntitySetId(),
                event.getNewPropertyTypes() );
    }

    @Timed
    public List<EntityKey> executeEntitySetDataSearchAcrossIndices(
            Map<UUID, UUID> entitySetAndSyncIds,
            Map<UUID, DelegatedStringSet> fieldSearches,
            int size,
            boolean explain ) {
        return elasticsearchApi.executeEntitySetDataSearchAcrossIndices( entitySetAndSyncIds,
                fieldSearches,
                size,
                explain );
    }

    @Timed
    public DataSearchResult executeAdvancedEntitySetDataSearch(
            UUID entitySetId,
            AdvancedSearch search,
            Set<UUID> authorizedProperties ) {
        List<SearchDetails> authorizedSearches = Lists.newArrayList();
        search.getSearches().forEach( searchDetails -> {
            if ( authorizedProperties.contains( searchDetails.getPropertyType() ) ) {
                authorizedSearches.add( searchDetails );
            }
        } );

        if ( !authorizedSearches.isEmpty() ) {
            UUID syncId = datasourceManager.getCurrentSyncId( entitySetId );
            SearchResult result = elasticsearchApi.executeAdvancedEntitySetDataSearch( entitySetId,
                    syncId,
                    authorizedSearches,
                    search.getStart(),
                    search.getMaxHits(),
                    authorizedProperties );

            Map<UUID, PropertyType> authorizedPropertyTypes = dataModelService
                    .getPropertyTypesAsMap( authorizedProperties );

            List<SetMultimap<Object, Object>> results = getResults( entitySetId,
                    syncId,
                    result,
                    authorizedPropertyTypes );
            return new DataSearchResult( result.getNumHits(), results );
        }

        return new DataSearchResult( 0, Lists.newArrayList() );
    }

    @Subscribe
    public void createEntityType( EntityTypeCreatedEvent event ) {
        EntityType entityType = event.getEntityType();
        elasticsearchApi.saveEntityTypeToElasticsearch( entityType );
    }

    @Subscribe
    public void createAssociationType( AssociationTypeCreatedEvent event ) {
        AssociationType associationType = event.getAssociationType();
        elasticsearchApi.saveAssociationTypeToElasticsearch( associationType );
    }

    @Subscribe
    public void createPropertyType( PropertyTypeCreatedEvent event ) {
        PropertyType propertyType = event.getPropertyType();
        elasticsearchApi.savePropertyTypeToElasticsearch( propertyType );
    }

    @Subscribe
    public void createApp( AppCreatedEvent event ) {
        App app = event.getApp();
        elasticsearchApi.saveAppToElasticsearch( app );
    }

    @Subscribe
    public void createAppType( AppTypeCreatedEvent event ) {
        AppType appType = event.getAppType();
        elasticsearchApi.saveAppTypeToElasticsearch( appType );
    }

    @Subscribe
    public void deleteEntityType( EntityTypeDeletedEvent event ) {
        UUID entityTypeId = event.getEntityTypeId();
        elasticsearchApi.deleteEntityType( entityTypeId );
    }

    @Subscribe
    public void deleteAssociationType( AssociationTypeDeletedEvent event ) {
        UUID associationTypeId = event.getAssociationTypeId();
        elasticsearchApi.deleteAssociationType( associationTypeId );
    }

    @Subscribe
    public void deletePropertyType( PropertyTypeDeletedEvent event ) {
        UUID propertyTypeId = event.getPropertyTypeId();
        elasticsearchApi.deletePropertyType( propertyTypeId );
    }

    @Subscribe
    public void deleteApp( AppDeletedEvent event ) {
        UUID appId = event.getAppId();
        elasticsearchApi.deleteApp( appId );
    }

    @Subscribe
    public void deleteAppType( AppTypeDeletedEvent event ) {
        UUID appTypeId = event.getAppTypeId();
        elasticsearchApi.deleteAppType( appTypeId );
    }

    public SearchResult executeEntityTypeSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executeEntityTypeSearch( searchTerm, start, maxHits );
    }

    public SearchResult executeAssociationTypeSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executeAssociationTypeSearch( searchTerm, start, maxHits );
    }

    public SearchResult executePropertyTypeSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executePropertyTypeSearch( searchTerm, start, maxHits );
    }

    public SearchResult executeAppSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executeAppSearch( searchTerm, start, maxHits );
    }

    public SearchResult executeAppTypeSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executeAppTypeSearch( searchTerm, start, maxHits );
    }

    public SearchResult executeFQNEntityTypeSearch( String namespace, String name, int start, int maxHits ) {
        return elasticsearchApi.executeFQNEntityTypeSearch( namespace, name, start, maxHits );
    }

    public SearchResult executeFQNPropertyTypeSearch( String namespace, String name, int start, int maxHits ) {
        return elasticsearchApi.executeFQNPropertyTypeSearch( namespace, name, start, maxHits );
    }

    @Timed
    public Map<UUID, List<NeighborEntityDetails>> executeEntityNeighborSearch( Set<UUID> entityKeyIds ) {
        Set<Principal> principals = Principals.getCurrentPrincipals();

        List<LoomEdge> edges = Lists.newArrayList();
        Set<UUID> entitySetIds = Sets.newHashSet();
        Map<UUID, Set<UUID>> authorizedEdgeESIdsToVertexESIds = Maps.newHashMap();
        Map<UUID, UUID> entityKeyIdToEntitySetId = Maps.newHashMap();
        Map<UUID, Map<UUID, PropertyType>> entitySetsIdsToAuthorizedProps = Maps.newHashMap();

        graphApi.getEdgesAndNeighborsForVertices( entityKeyIds ).forEach( edge -> {

            edges.add( edge );
            entitySetIds.add( edge.getEdgeSetId() );
            entitySetIds.add( entityKeyIds.contains( edge.getSrcEntityKeyId() ) ? edge.getDstSetId()
                    : edge.getSrcSetId() );
        } );

        Set<UUID> authorizedEntitySetIds = authorizations.accessChecksForPrincipals( entitySetIds.stream()
                .map( entitySetId -> new AccessCheck( new AclKey( entitySetId ), EnumSet.of( Permission.READ ) ) )
                .collect( Collectors.toSet() ), principals )
                .filter( auth -> auth.getPermissions().get( Permission.READ ) ).map( auth -> auth.getAclKey().get( 0 ) )
                .collect( Collectors.toSet() );

        Map<UUID, EntitySet> entitySetsById = dataModelService.getEntitySetsAsMap( authorizedEntitySetIds );

        Map<UUID, EntityType> entityTypesById = dataModelService
                .getEntityTypesAsMap( entitySetsById.values().stream().map( entitySet -> {
                    entitySetsIdsToAuthorizedProps.put( entitySet.getId(), Maps.newHashMap() );
                    authorizedEdgeESIdsToVertexESIds.put( entitySet.getId(), Sets.newHashSet() );
                    return entitySet.getEntityTypeId();
                } ).collect( Collectors.toSet() ) );

        Map<UUID, PropertyType> propertyTypesById = dataModelService
                .getPropertyTypesAsMap( entityTypesById.values().stream()
                        .flatMap( entityType -> entityType.getProperties().stream() ).collect(
                                Collectors.toSet() ) );

        Set<AccessCheck> accessChecks = entitySetsById.values().stream()
                .flatMap( entitySet -> entityTypesById.get( entitySet.getEntityTypeId() ).getProperties().stream()
                        .map( propertyTypeId -> new AccessCheck( new AclKey( entitySet.getId(), propertyTypeId ),
                                EnumSet.of( Permission.READ ) ) ) ).collect( Collectors.toSet() );

        authorizations.accessChecksForPrincipals( accessChecks, principals ).forEach( auth -> {
            if ( auth.getPermissions().get( Permission.READ ) ) {
                UUID entitySetId = auth.getAclKey().get( 0 );
                UUID propertyTypeId = auth.getAclKey().get( 1 );
                entitySetsIdsToAuthorizedProps.get( entitySetId )
                        .put( propertyTypeId, propertyTypesById.get( propertyTypeId ) );
            }
        } );

        edges.forEach( edge -> {

            UUID edgeEntityKeyId = edge.getEdgeEntityKeyId();
            UUID neighborEntityKeyId = ( entityKeyIds.contains( edge.getSrcEntityKeyId() ) ) ? edge.getDstEntityKeyId()
                    : edge.getSrcEntityKeyId();
            UUID edgeEntitySetId = edge.getEdgeSetId();
            UUID neighborEntitySetId = ( entityKeyIds.contains( edge.getSrcEntityKeyId() ) ) ? edge.getDstSetId()
                    : edge.getSrcSetId();

            if ( entitySetsIdsToAuthorizedProps.containsKey( edgeEntitySetId ) ) {
                entityKeyIdToEntitySetId.put( edgeEntityKeyId, edgeEntitySetId );

                if ( entitySetsIdsToAuthorizedProps.containsKey( neighborEntitySetId ) ) {
                    authorizedEdgeESIdsToVertexESIds.get( edgeEntitySetId ).add( neighborEntitySetId );
                    entityKeyIdToEntitySetId.put( neighborEntityKeyId, neighborEntitySetId );
                }
            }

        } );

        Map<UUID, SetMultimap<FullQualifiedName, Object>> entities = dataManager
                .getEntitiesAcrossEntitySets( entityKeyIdToEntitySetId, entitySetsIdsToAuthorizedProps );

        Map<UUID, List<NeighborEntityDetails>> entityNeighbors = Maps.newConcurrentMap();

        // create a NeighborEntityDetails object for each edge based on authorizations
        edges.parallelStream().forEach( edge -> {
            boolean vertexIsSrc = entityKeyIds.contains( edge.getKey().getSrcEntityKeyId() );
            UUID entityId = ( vertexIsSrc ) ? edge.getKey().getSrcEntityKeyId() : edge.getKey().getDstEntityKeyId();
            if ( !entityNeighbors.containsKey( entityId ) ) {
                entityNeighbors.put( entityId, Collections.synchronizedList( Lists.newArrayList() ) );
            }
            NeighborEntityDetails neighbor = getNeighborEntityDetails( edge,
                    authorizedEdgeESIdsToVertexESIds,
                    entitySetsIdsToAuthorizedProps,
                    entitySetsById,
                    vertexIsSrc,
                    entities );
            if ( neighbor != null ) {
                entityNeighbors.get( entityId ).add( neighbor );
            }
        } );

        return entityNeighbors;

    }

    private boolean getAuthorization( UUID entitySetId, Set<Principal> principals ) {
        return authorizations.accessChecksForPrincipals( ImmutableSet
                .of( new AccessCheck( new AclKey( entitySetId ), EnumSet.of( Permission.READ ) ) ), principals )
                .findFirst().get().getPermissions().get( Permission.READ );
    }

    private Map<UUID, PropertyType> getAuthorizedProperties( UUID entitySetId ) {
        return authzHelper
                .getAuthorizedPropertiesOnEntitySet( entitySetId,
                        EnumSet.of( Permission.READ ) )
                .stream()
                .collect( Collectors.toMap( ptId -> ptId,
                        ptId -> dataModelService.getPropertyType( ptId ) ) );
    }

    private NeighborEntityDetails getNeighborEntityDetails(
            LoomEdge edge,
            Map<UUID, Set<UUID>> authorizedEdgeESIdsToVertexESIds,
            Map<UUID, Map<UUID, PropertyType>> entitySetsIdsToAuthorizedProps,
            Map<UUID, EntitySet> entitySetsById,
            boolean vertexIsSrc,
            Map<UUID, SetMultimap<FullQualifiedName, Object>> entities ) {

        UUID edgeEntitySetId = edge.getEdgeSetId();
        UUID neighborEntityKeyId = ( vertexIsSrc ) ? edge.getDstEntityKeyId() : edge.getSrcEntityKeyId();
        UUID neighborEntitySetId = ( vertexIsSrc ) ? edge.getDstSetId() : edge.getSrcSetId();

        SetMultimap<FullQualifiedName, Object> edgeDetails = entities.get( edge.getEdgeEntityKeyId() );
        if ( authorizedEdgeESIdsToVertexESIds.get( edgeEntitySetId )
                .contains( neighborEntitySetId ) ) {
            SetMultimap<FullQualifiedName, Object> neighborDetails = entities.get( neighborEntityKeyId );
            return new NeighborEntityDetails(
                    entitySetsById.get( edgeEntitySetId ),
                    edgeDetails,
                    entitySetsIdsToAuthorizedProps.get( edgeEntitySetId ).values(),
                    entitySetsById.get( neighborEntitySetId ),
                    neighborEntityKeyId,
                    neighborDetails,
                    entitySetsIdsToAuthorizedProps.get( neighborEntitySetId ).values(),
                    vertexIsSrc );
        } else {
            return new NeighborEntityDetails(
                    entitySetsById.get( edgeEntitySetId ),
                    edgeDetails,
                    entitySetsIdsToAuthorizedProps.get( edgeEntitySetId ).values(),
                    vertexIsSrc );
        }
    }

    private List<SetMultimap<Object, Object>> getResults(
            UUID entitySetId,
            UUID syncId,
            SearchResult result,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        Collection<UUID> ids = entityKeyService.getEntityKeyIds( result.getHits().parallelStream()
                .map( hit -> (String) hit.get( "id" ) )
                .map( id -> new EntityKey( entitySetId, id, syncId ) )
                .collect( Collectors.toSet() ) ).values();
        return dataManager.getEntities( ids, authorizedPropertyTypes ).collect( Collectors.toList() );
    }

    @Subscribe
    public void clearAllData( ClearAllDataEvent event ) {
        elasticsearchApi.clearAllData();
    }

    public void triggerPropertyTypeIndex( List<PropertyType> propertyTypes ) {
        elasticsearchApi.triggerPropertyTypeIndex( propertyTypes );
    }

    public void triggerEntityTypeIndex( List<EntityType> entityTypes ) {
        elasticsearchApi.triggerEntityTypeIndex( entityTypes );
    }

    public void triggerAssociationTypeIndex( List<AssociationType> associationTypes ) {
        elasticsearchApi.triggerAssociationTypeIndex( associationTypes );
    }

    public void triggerEntitySetIndex() {
        Map<EntitySet, Set<UUID>> entitySets = StreamUtil.stream( dataModelService.getEntitySets() ).collect( Collectors
                .toMap( entitySet -> entitySet,
                        entitySet -> dataModelService.getEntityType( entitySet.getEntityTypeId() ).getProperties() ) );
        Map<UUID, PropertyType> propertyTypes = StreamUtil.stream( dataModelService.getPropertyTypes() )
                .collect( Collectors.toMap( pt -> pt.getId(), pt -> pt ) );
        elasticsearchApi.triggerEntitySetIndex( entitySets, propertyTypes );
    }

    public void triggerEntitySetDataIndex( UUID entitySetId ) {
        Set<UUID> propertyTypeIds = dataModelService.getEntityTypeByEntitySetId( entitySetId ).getProperties();
        Map<UUID, PropertyType> propertyTypes = dataModelService.getPropertyTypesAsMap( propertyTypeIds );
        List<PropertyType> propertyTypeList = Lists.newArrayList( propertyTypes.values() );
        UUID syncId = datasourceManager.getCurrentSyncId( entitySetId );

        elasticsearchApi.deleteEntitySet( entitySetId );
        elasticsearchApi.saveEntitySetToElasticsearch( dataModelService.getEntitySet( entitySetId ), propertyTypeList );
        elasticsearchApi.createSecurableObjectIndex( entitySetId, syncId, propertyTypeList );

        Map<FullQualifiedName, UUID> propertyIdsByFqn = propertyTypes.values().stream()
                .collect( Collectors.toMap( pt -> pt.getType(), pt -> pt.getId() ) );

        Set<FullQualifiedName> ignore = propertyTypeList.stream()
                .filter( pt -> pt.getDatatype().equals( EdmPrimitiveTypeKind.Binary ) )
                .map( pt -> pt.getType() )
                .collect( Collectors.toSet() );

        dataManager.getEntityKeysForEntitySet( entitySetId, syncId )
                .parallel()
                .forEach( entityKey -> {
                    String entityId = entityKey.getEntityId();
                    SetMultimap<UUID, Object> entity = HashMultimap.create();
                    dataManager
                            .getEntity( entitySetId, syncId, entityId, propertyTypes )
                            .entries().stream()
                            .filter( entry -> !ignore.contains( entry.getKey() ) )
                            .forEach( entry -> entity.put( propertyIdsByFqn.get( entry.getKey() ), entry.getValue() ) );
                    elasticsearchApi
                            .createEntityData( entitySetId, syncId, entityId, entity );
                } );
    }

    public void triggerAllEntitySetDataIndex() {
        dataModelService.getEntitySets().forEach( entitySet -> triggerEntitySetDataIndex( entitySet.getId() ) );
    }

    public void triggerAppIndex( List<App> apps ) {
        elasticsearchApi.triggerAppIndex( apps );
    }

    public void triggerAppTypeIndex( List<AppType> appTypes ) {
        elasticsearchApi.triggerAppTypeIndex( appTypes );
    }

}
