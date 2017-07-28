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
import com.dataloom.authorization.AbstractSecurableObjectResolveTypeService;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.EdmAuthorizationHelper;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.Principals;
import com.dataloom.authorization.events.AclUpdateEvent;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.data.DatasourceManager;
import com.dataloom.data.EntityKey;
import com.dataloom.data.EntityKeyIdService;
import com.dataloom.data.events.EntityDataCreatedEvent;
import com.dataloom.data.events.EntityDataDeletedEvent;
import com.dataloom.data.requests.NeighborEntityDetails;
import com.dataloom.data.storage.CassandraEntityDatastore;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.events.AssociationTypeCreatedEvent;
import com.dataloom.edm.events.AssociationTypeDeletedEvent;
import com.dataloom.edm.events.EntitySetCreatedEvent;
import com.dataloom.edm.events.EntitySetDeletedEvent;
import com.dataloom.edm.events.EntitySetMetadataUpdatedEvent;
import com.dataloom.edm.events.EntityTypeCreatedEvent;
import com.dataloom.edm.events.EntityTypeDeletedEvent;
import com.dataloom.edm.events.PropertyTypeCreatedEvent;
import com.dataloom.edm.events.PropertyTypeDeletedEvent;
import com.dataloom.edm.events.PropertyTypesInEntitySetUpdatedEvent;
import com.dataloom.edm.type.AssociationType;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.core.LoomGraphApi;
import com.dataloom.graph.edge.LoomEdge;
import com.dataloom.linking.Entity;
import com.dataloom.organizations.events.OrganizationCreatedEvent;
import com.dataloom.organizations.events.OrganizationDeletedEvent;
import com.dataloom.organizations.events.OrganizationUpdatedEvent;
import com.dataloom.search.requests.AdvancedSearch;
import com.dataloom.search.requests.DataSearchResult;
import com.dataloom.search.requests.SearchDetails;
import com.dataloom.search.requests.SearchResult;
import com.dataloom.search.requests.SearchTerm;
import com.dataloom.sync.events.CurrentSyncUpdatedEvent;
import com.dataloom.sync.events.SyncIdCreatedEvent;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.kryptnostic.datastore.services.EdmManager;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        return elasticsearchApi.executeEntitySetMetadataSearch( optionalQuery,
                optionalEntityType,
                optionalPropertyTypes,
                null,
                start,
                maxHits );
    }

    public void updateEntitySetPermissions( List<UUID> aclKeys, Principal principal, Set<Permission> permissions ) {
        aclKeys.forEach( aclKey -> elasticsearchApi.updateEntitySetPermissions( aclKey, principal, permissions ) );
    }

    public void updateOrganizationPermissions( List<UUID> aclKeys, Principal principal, Set<Permission> permissions ) {
        aclKeys.forEach( aclKey -> elasticsearchApi.updateOrganizationPermissions( aclKey, principal, permissions ) );
    }

    @Subscribe
    public void onAclUpdate( AclUpdateEvent event ) {
        SecurableObjectType type = securableObjectTypes.getSecurableObjectType( event.getAclKeys() );
        if ( type == SecurableObjectType.EntitySet ) {
            event.getPrincipals().forEach( principal -> updateEntitySetPermissions(
                    event.getAclKeys(),
                    principal,
                    authorizations.getSecurableObjectPermissions( event.getAclKeys(),
                            Sets.newHashSet( principal ) ) ) );
        } else if ( type == SecurableObjectType.Organization ) {
            event.getPrincipals().forEach( principal -> updateOrganizationPermissions(
                    event.getAclKeys(),
                    principal,
                    authorizations.getSecurableObjectPermissions( event.getAclKeys(),
                            Sets.newHashSet( principal ) ) ) );
        }
    }

    @Subscribe
    public void createEntitySet( EntitySetCreatedEvent event ) {
        elasticsearchApi.saveEntitySetToElasticsearch( event.getEntitySet(),
                event.getPropertyTypes(),
                event.getPrincipal() );
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
        elasticsearchApi.createOrganization( event.getOrganization(), event.getPrincipal() );
    }

    public SearchResult executeOrganizationKeywordSearch( SearchTerm searchTerm ) {
        return elasticsearchApi.executeOrganizationSearch( searchTerm.getSearchTerm(),
                null,
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
        UUID syncId = ( event.getOptionalSyncId().isPresent() ) ? event.getOptionalSyncId().get()
                : datasourceManager.getCurrentSyncId( event.getEntitySetId() );
        elasticsearchApi.createEntityData( event.getEntitySetId(),
                syncId,
                event.getEntityId(),
                event.getPropertyValues() );
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
        Map<UUID, PropertyType> authorizedPropertyTypes = Maps.newHashMap();
        authorizedProperties.forEach( id -> authorizedPropertyTypes.put( id, dataModelService.getPropertyType( id ) ) );
        Set<EntityKey> entityKeys = result.getHits()
                .parallelStream()
                .map( hit -> new EntityKey( entitySetId, hit.get( "id" ).toString(), syncId ) )
                .collect( Collectors.toSet() );
        List<SetMultimap<Object, Object>> results = entityKeyService.getEntityKeyIds( entityKeys )
                .values()
                .parallelStream()
                .map( entityKeyId -> {
                    SetMultimap<Object, Object> fullRow = HashMultimap
                            .create( dataManager.getEntityById( entityKeyId, authorizedPropertyTypes ) );
                    fullRow.put( "id", entityKeyId.toString() );
                    return fullRow;
                } )
                .collect( Collectors.toList() );

        return new DataSearchResult( result.getNumHits(), results );
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
    public List<Entity> executeEntitySetDataSearchAcrossIndices(
            Map<UUID, UUID> entitySetAndSyncIds,
            Map<UUID, Set<String>> fieldSearches,
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

            Map<UUID, PropertyType> authorizedPropertyTypes = Maps.newHashMap();
            authorizedProperties
                    .forEach( id -> authorizedPropertyTypes.put( id, dataModelService.getPropertyType( id ) ) );
            List<SetMultimap<Object, Object>> results = result.getHits().stream().map( hit -> {
                String entityId = hit.get( "id" ).toString();
                UUID vertexId = entityKeyService.getEntityKeyId( new EntityKey( entitySetId, entityId, syncId ) );
                SetMultimap<Object, Object> fullRow = HashMultimap
                        .create( dataManager.getEntity( entitySetId, syncId, entityId, authorizedPropertyTypes ) );
                fullRow.put( "id", vertexId.toString() );
                return fullRow;
            } ).collect( Collectors.toList() );
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

    public SearchResult executeEntityTypeSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executeEntityTypeSearch( searchTerm, start, maxHits );
    }

    public SearchResult executeAssociationTypeSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executeAssociationTypeSearch( searchTerm, start, maxHits );
    }

    public SearchResult executePropertyTypeSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executePropertyTypeSearch( searchTerm, start, maxHits );
    }

    public SearchResult executeFQNEntityTypeSearch( String namespace, String name, int start, int maxHits ) {
        return elasticsearchApi.executeFQNEntityTypeSearch( namespace, name, start, maxHits );
    }

    public SearchResult executeFQNPropertyTypeSearch( String namespace, String name, int start, int maxHits ) {
        return elasticsearchApi.executeFQNPropertyTypeSearch( namespace, name, start, maxHits );
    }

    @Timed
    public List<NeighborEntityDetails> executeEntityNeighborSearch( UUID entityId ) {
        List<LoomEdge> edges = graphApi.getEdgesAndNeighborsForVertex( entityId ).collect( Collectors.toList() );
        Map<UUID, EntityKey> entityKeyIdToEntityKey = Maps.newHashMap();
        Map<UUID, Set<UUID>> edgeESIdsToVertexESIds = Maps.newHashMap();
        Map<UUID, Set<UUID>> authorizedEdgeESIdsToVertexESIds = Maps.newHashMap();
        Map<UUID, Map<UUID, PropertyType>> entitySetsIdsToAuthorizedProps = Maps.newHashMap();
        Map<UUID, EntitySet> entitySetsById = Maps.newHashMap();

        // map entity key ids to entity set ids, and map each edge type to all neighbor vertex types connected by that
        // edge type
        edges.forEach( edge -> {
            boolean vertexIsSrc = entityId.equals( edge.getKey().getSrcEntityKeyId() );
            EntityKey edgeEntityKey = entityKeyService.getEntityKey( edge.getKey().getEdgeEntityKeyId() );
            UUID neighborId = ( vertexIsSrc ) ? edge.getKey().getDstEntityKeyId() : edge.getKey().getSrcEntityKeyId();
            EntityKey neighborEntityKey = entityKeyService.getEntityKey( neighborId );
            entityKeyIdToEntityKey.put( edge.getKey().getEdgeEntityKeyId(), edgeEntityKey );
            entityKeyIdToEntityKey.put( neighborId, neighborEntityKey );
            if ( edgeESIdsToVertexESIds.containsKey( edgeEntityKey.getEntitySetId() ) ) {
                edgeESIdsToVertexESIds.get( edgeEntityKey.getEntitySetId() ).add( neighborEntityKey.getEntitySetId() );
            } else {
                edgeESIdsToVertexESIds.put( edgeEntityKey.getEntitySetId(),
                        Sets.newHashSet( neighborEntityKey.getEntitySetId() ) );
            }

        } );

        // filter to only authorized entity sets, and load entity set and property type info for authorized ones
        edgeESIdsToVertexESIds.entrySet().forEach( entry -> {
            if ( authorizations.checkIfHasPermissions( ImmutableList.of( entry.getKey() ),
                    Principals.getCurrentPrincipals(),
                    EnumSet.of( Permission.READ ) ) ) {
                if ( !authorizedEdgeESIdsToVertexESIds.containsKey( entry.getKey() ) ) {
                    authorizedEdgeESIdsToVertexESIds.put( entry.getKey(), Sets.newHashSet() );
                    entitySetsById.put( entry.getKey(), dataModelService.getEntitySet( entry.getKey() ) );
                    entitySetsIdsToAuthorizedProps.put( entry.getKey(), getAuthorizedProperties( entry.getKey() ) );
                }

                entry.getValue().forEach( vertexTypeId -> {
                    if ( authorizations.checkIfHasPermissions( ImmutableList.of( vertexTypeId ),
                            Principals.getCurrentPrincipals(),
                            EnumSet.of( Permission.READ ) ) ) {
                        authorizedEdgeESIdsToVertexESIds.get( entry.getKey() ).add( vertexTypeId );
                        if ( !entitySetsById.containsKey( vertexTypeId ) ) {
                            entitySetsById.put( vertexTypeId, dataModelService.getEntitySet( vertexTypeId ) );
                            entitySetsIdsToAuthorizedProps.put( vertexTypeId, getAuthorizedProperties( vertexTypeId ) );
                        }
                    }
                } );
            }
        } );

        List<NeighborEntityDetails> neighbors = Lists.newArrayList();

        // create a NeighborEntityDetails object for each edge based on authorizations
        edges.forEach( edge -> {
            boolean vertexIsSrc = entityId.equals( edge.getKey().getSrcEntityKeyId() );
            NeighborEntityDetails neighbor = getNeighborEntityDetails( edge,
                    entityKeyIdToEntityKey,
                    authorizedEdgeESIdsToVertexESIds,
                    entitySetsIdsToAuthorizedProps,
                    entitySetsById,
                    vertexIsSrc );
            if ( neighbor != null ) { neighbors.add( neighbor ); }
        } );

        return neighbors;

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
            Map<UUID, EntityKey> entityKeyIdToEntityKey,
            Map<UUID, Set<UUID>> authorizedEdgeESIdsToVertexESIds,
            Map<UUID, Map<UUID, PropertyType>> entitySetsIdsToAuthorizedProps,
            Map<UUID, EntitySet> entitySetsById,
            boolean vertexIsSrc ) {
        EntityKey edgeEntityKey = entityKeyIdToEntityKey.get( edge.getKey().getEdgeEntityKeyId() );
        UUID edgeEntitySetId = edgeEntityKey.getEntitySetId();
        UUID neighborEntityKeyId = ( vertexIsSrc ) ? edge.getKey().getDstEntityKeyId()
                : edge.getKey().getSrcEntityKeyId();
        EntityKey neighborEntityKey = entityKeyIdToEntityKey.get( neighborEntityKeyId );

        if ( authorizedEdgeESIdsToVertexESIds.containsKey( edgeEntitySetId ) ) {
            SetMultimap<FullQualifiedName, Object> edgeDetails = dataManager.getEntity( edgeEntitySetId,
                    edgeEntityKey.getSyncId(),
                    edgeEntityKey.getEntityId(),
                    entitySetsIdsToAuthorizedProps.get( edgeEntitySetId ) );
            if ( authorizedEdgeESIdsToVertexESIds.get( edgeEntitySetId )
                    .contains( neighborEntityKey.getEntitySetId() ) ) {
                SetMultimap<FullQualifiedName, Object> neighborDetails = dataManager.getEntity(
                        neighborEntityKey.getEntitySetId(),
                        neighborEntityKey.getSyncId(),
                        neighborEntityKey.getEntityId(),
                        entitySetsIdsToAuthorizedProps.get( neighborEntityKey.getEntitySetId() ) );
                return new NeighborEntityDetails(
                        entitySetsById.get( edgeEntitySetId ),
                        edgeDetails,
                        entitySetsIdsToAuthorizedProps.get( edgeEntitySetId ).values(),
                        entitySetsById.get( neighborEntityKey.getEntitySetId() ),
                        neighborEntityKeyId,
                        neighborDetails,
                        entitySetsIdsToAuthorizedProps.get( neighborEntityKey.getEntitySetId() ).values(),
                        vertexIsSrc );
            } else {
                return new NeighborEntityDetails(
                        entitySetsById.get( edgeEntitySetId ),
                        edgeDetails,
                        entitySetsIdsToAuthorizedProps.get( edgeEntitySetId ).values(),
                        vertexIsSrc );
            }
        }

        return null;
    }

}
