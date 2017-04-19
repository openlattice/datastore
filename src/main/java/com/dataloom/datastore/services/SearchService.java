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
import com.dataloom.data.events.EntityDataCreatedEvent;
import com.dataloom.data.events.EntityDataDeletedEvent;
import com.dataloom.data.ids.CassandraEntityKeyIdService;
import com.dataloom.data.requests.NeighborEntityDetails;
import com.dataloom.data.storage.CassandraEntityDatastore;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.events.EntitySetCreatedEvent;
import com.dataloom.edm.events.EntitySetDeletedEvent;
import com.dataloom.edm.events.EntitySetMetadataUpdatedEvent;
import com.dataloom.edm.events.EntityTypeCreatedEvent;
import com.dataloom.edm.events.EntityTypeDeletedEvent;
import com.dataloom.edm.events.PropertyTypeCreatedEvent;
import com.dataloom.edm.events.PropertyTypeDeletedEvent;
import com.dataloom.edm.events.PropertyTypesInEntitySetUpdatedEvent;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.graph.core.LoomGraphApi;
import com.dataloom.graph.core.objects.LoomEdgeKey;
import com.dataloom.linking.Entity;
import com.dataloom.organizations.events.OrganizationCreatedEvent;
import com.dataloom.organizations.events.OrganizationDeletedEvent;
import com.dataloom.organizations.events.OrganizationUpdatedEvent;
import com.dataloom.search.requests.AdvancedSearch;
import com.dataloom.search.requests.SearchResult;
import com.dataloom.search.requests.SearchTerm;
import com.dataloom.sync.events.CurrentSyncUpdatedEvent;
import com.dataloom.sync.events.SyncIdCreatedEvent;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.kryptnostic.datastore.services.EdmManager;

public class SearchService {
    private static final Logger                       logger = LoggerFactory.getLogger( SearchService.class );

    @Inject
    private EventBus                                  eventBus;

    @Inject
    private AuthorizationManager                      authorizations;

    @Inject
    private AbstractSecurableObjectResolveTypeService securableObjectTypes;

    @Inject
    private DatastoreConductorElasticsearchApi        elasticsearchApi;

    @Inject
    private DatasourceManager                         datasourceManager;

    @Inject
    private EdmManager                                dataModelService;

    @Inject
    private LoomGraphApi                              graphApi;

    @Inject
    private CassandraEntityDatastore                  dataManager;

    @Inject
    private EdmAuthorizationHelper                    authzHelper;

    @Inject
    private CassandraEntityKeyIdService               entityKeyService;

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

    public SearchResult executeEntitySetDataSearch(
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
        result.getHits().replaceAll( hit -> {
            String entityId = hit.get( "id" ).toString();
            UUID vertexId = graphApi.getVertexId( new EntityKey( entitySetId, entityId, syncId ) );
            hit.put( "id", vertexId.toString() );
            return hit;
        } );
        return result;
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

    public SearchResult executeAdvancedEntitySetDataSearch(
            UUID entitySetId,
            AdvancedSearch search,
            Set<UUID> authorizedProperties ) {
        Map<UUID, String> authorizedSearches = Maps.newHashMap();
        search.getSearches().entrySet().forEach( entry -> {
            if ( authorizedProperties.contains( entry.getKey() ) )
                authorizedSearches.put( entry.getKey(), entry.getValue() );
        } );

        if ( !authorizedSearches.isEmpty() ) {
            UUID syncId = datasourceManager.getCurrentSyncId( entitySetId );
            return elasticsearchApi.executeAdvancedEntitySetDataSearch( entitySetId,
                    syncId,
                    authorizedSearches,
                    search.getStart(),
                    search.getMaxHits(),
                    authorizedProperties );
        }

        return new SearchResult( 0, Lists.newArrayList() );
    }

    @Subscribe
    public void createEntityType( EntityTypeCreatedEvent event ) {
        EntityType entityType = event.getEntityType();
        elasticsearchApi.saveEntityTypeToElasticsearch( entityType );
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
    public void deletePropertyType( PropertyTypeDeletedEvent event ) {
        UUID propertyTypeId = event.getPropertyTypeId();
        elasticsearchApi.deletePropertyType( propertyTypeId );
    }

    public SearchResult executeEntityTypeSearch( String searchTerm, int start, int maxHits ) {
        return elasticsearchApi.executeEntityTypeSearch( searchTerm, start, maxHits );
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

    public List<NeighborEntityDetails> executeEntityNeighborSearch( UUID entitySetId, UUID entityId ) {
        Pair<List<LoomEdgeKey>, List<LoomEdgeKey>> srcAndDstEdges = graphApi.getEdgesAndNeighborsForVertex( entityId );
        Map<UUID, EntityKey> entityKeyIdToEntityKey = Maps.newHashMap();
        Map<UUID, Set<UUID>> edgeESIdsToVertexESIds = Maps.newHashMap();
        Map<UUID, Set<UUID>> authorizedEdgeESIdsToVertexESIds = Maps.newHashMap();
        Map<UUID, Map<UUID, PropertyType>> entitySetsIdsToAuthorizedProps = Maps.newHashMap();
        Map<UUID, EntitySet> entitySetsById = Maps.newHashMap();

        // map entity key ids to entity set ids, and map each edge type to all neighbor vertex types connected by that
        // edge type
        srcAndDstEdges.getLeft().forEach( srcEdge -> {
            EntityKey edgeEntityKey = entityKeyService.getEntityKey( srcEdge.getKey().getEdgeEntityKeyId() );
            EntityKey vertexEntityKey = entityKeyService.getEntityKey( srcEdge.getKey().getDstEntityKeyId() );
            entityKeyIdToEntityKey.put( srcEdge.getKey().getEdgeEntityKeyId(), edgeEntityKey );
            entityKeyIdToEntityKey.put( srcEdge.getKey().getDstEntityKeyId(), vertexEntityKey );
            if ( edgeESIdsToVertexESIds.containsKey( edgeEntityKey.getEntitySetId() ) ) {
                edgeESIdsToVertexESIds.get( edgeEntityKey.getEntitySetId() ).add( vertexEntityKey.getEntitySetId() );
            } else {
                edgeESIdsToVertexESIds.put( edgeEntityKey.getEntitySetId(),
                        Sets.newHashSet( vertexEntityKey.getEntitySetId() ) );
            }
        } );
        srcAndDstEdges.getRight().forEach( srcEdge -> {
            EntityKey edgeEntityKey = entityKeyService.getEntityKey( srcEdge.getKey().getEdgeEntityKeyId() );
            EntityKey vertexEntityKey = entityKeyService.getEntityKey( srcEdge.getKey().getSrcEntityKeyId() );
            entityKeyIdToEntityKey.put( srcEdge.getKey().getEdgeEntityKeyId(), edgeEntityKey );
            entityKeyIdToEntityKey.put( srcEdge.getKey().getSrcEntityKeyId(), vertexEntityKey );
            if ( edgeESIdsToVertexESIds.containsKey( edgeEntityKey.getEntitySetId() ) ) {
                edgeESIdsToVertexESIds.get( edgeEntityKey.getEntitySetId() ).add( vertexEntityKey.getEntitySetId() );
            } else {
                edgeESIdsToVertexESIds.put( edgeEntityKey.getEntitySetId(),
                        Sets.newHashSet( vertexEntityKey.getEntitySetId() ) );
            }
        } );

        // filter to only authorized entity sets, and load entity set and property type info for authorized ones
        edgeESIdsToVertexESIds.entrySet().forEach( entry -> {
            if ( authorizations.checkIfHasPermissions( ImmutableList.of( entry.getKey() ),
                    Principals.getCurrentPrincipals(),
                    EnumSet.of( Permission.READ ) ) ) {
                if ( !authorizedEdgeESIdsToVertexESIds.containsKey( entry.getKey() ) ) {
                    authorizedEdgeESIdsToVertexESIds.put( entry.getKey(), Sets.newHashSet() );

                    if ( !entitySetsById.containsKey( entry.getKey() ) )
                        entitySetsById.put( entry.getKey(), dataModelService.getEntitySet( entry.getKey() ) );

                    Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper
                            .getAuthorizedPropertiesOnEntitySet( entry.getKey(),
                                    EnumSet.of( Permission.READ ) )
                            .stream()
                            .collect( Collectors.toMap( ptId -> ptId,
                                    ptId -> dataModelService.getPropertyType( ptId ) ) );
                    entitySetsIdsToAuthorizedProps.put( entry.getKey(), authorizedPropertyTypes );
                }

                entry.getValue().forEach( vertexTypeId -> {
                    if ( authorizations.checkIfHasPermissions( ImmutableList.of( vertexTypeId ),
                            Principals.getCurrentPrincipals(),
                            EnumSet.of( Permission.READ ) ) ) {
                        authorizedEdgeESIdsToVertexESIds.get( entry.getKey() ).add( vertexTypeId );

                        if ( !entitySetsById.containsKey( vertexTypeId ) )
                            entitySetsById.put( vertexTypeId, dataModelService.getEntitySet( vertexTypeId ) );

                        if ( !entitySetsIdsToAuthorizedProps.containsKey( vertexTypeId ) ) {
                            Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper
                                    .getAuthorizedPropertiesOnEntitySet( vertexTypeId,
                                            EnumSet.of( Permission.READ ) )
                                    .stream()
                                    .collect( Collectors.toMap( ptId -> ptId,
                                            ptId -> dataModelService.getPropertyType( ptId ) ) );
                            entitySetsIdsToAuthorizedProps.put( vertexTypeId, authorizedPropertyTypes );
                        }
                    }
                } );
            }
        } );

        List<NeighborEntityDetails> neighbors = Lists.newArrayList();

        // create a NeighborEntityDetails object for each edge based on authorizations
        srcAndDstEdges.getLeft().forEach( srcEdge -> {
            NeighborEntityDetails neighbor = getNeighborEntityDetails( srcEdge,
                    entityKeyIdToEntityKey,
                    authorizedEdgeESIdsToVertexESIds,
                    entitySetsIdsToAuthorizedProps,
                    entitySetsById,
                    true );
            if ( neighbor != null ) neighbors.add( neighbor );
        } );

        srcAndDstEdges.getRight().forEach( dstEdge -> {
            NeighborEntityDetails neighbor = getNeighborEntityDetails( dstEdge,
                    entityKeyIdToEntityKey,
                    authorizedEdgeESIdsToVertexESIds,
                    entitySetsIdsToAuthorizedProps,
                    entitySetsById,
                    false );
            if ( neighbor != null ) neighbors.add( neighbor );
        } );

        return neighbors;

    }

    private NeighborEntityDetails getNeighborEntityDetails(
            LoomEdgeKey edge,
            Map<UUID, EntityKey> entityKeyIdToEntityKey,
            Map<UUID, Set<UUID>> authorizedEdgeESIdsToVertexESIds,
            Map<UUID, Map<UUID, PropertyType>> entitySetsIdsToAuthorizedProps,
            Map<UUID, EntitySet> entitySetsById,
            boolean isSrc ) {
        EntityKey edgeEntityKey = entityKeyIdToEntityKey.get( edge.getKey().getEdgeEntityKeyId() );
        UUID edgeEntitySetId = edgeEntityKey.getEntitySetId();
        UUID vertexEntityKeyId = ( isSrc ) ? edge.getKey().getDstEntityKeyId() : edge.getKey().getSrcEntityKeyId();
        EntityKey vertexEntityKey = entityKeyIdToEntityKey.get( vertexEntityKeyId );

        if ( authorizedEdgeESIdsToVertexESIds.containsKey( edgeEntitySetId ) ) {
            SetMultimap<FullQualifiedName, Object> edgeDetails = dataManager.getEntity( edgeEntitySetId,
                    edgeEntityKey.getSyncId(),
                    edgeEntityKey.getEntityId(),
                    entitySetsIdsToAuthorizedProps.get( edgeEntitySetId ) );
            if ( authorizedEdgeESIdsToVertexESIds.get( edgeEntitySetId )
                    .contains( vertexEntityKey.getEntitySetId() ) ) {
                SetMultimap<FullQualifiedName, Object> vertexDetails = dataManager.getEntity(
                        vertexEntityKey.getEntitySetId(),
                        vertexEntityKey.getSyncId(),
                        vertexEntityKey.getEntityId(),
                        entitySetsIdsToAuthorizedProps.get( vertexEntityKey.getEntitySetId() ) );
                return new NeighborEntityDetails(
                        entitySetsById.get( edgeEntitySetId ),
                        edgeDetails,
                        entitySetsIdsToAuthorizedProps.get( edgeEntitySetId ).values(),
                        entitySetsById.get( vertexEntityKey.getEntitySetId() ),
                        vertexEntityKeyId,
                        vertexDetails,
                        entitySetsIdsToAuthorizedProps.get( vertexEntityKey.getEntitySetId() ).values(),
                        isSrc );
            } else {
                return new NeighborEntityDetails(
                        entitySetsById.get( edgeEntitySetId ),
                        edgeDetails,
                        entitySetsIdsToAuthorizedProps.get( edgeEntitySetId ).values(),
                        isSrc );
            }
        }

        return null;
    }

}
