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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.AbstractSecurableObjectResolveTypeService;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.events.AclUpdateEvent;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.data.events.EntityDataCreatedEvent;
import com.dataloom.edm.events.EntitySetCreatedEvent;
import com.dataloom.edm.events.EntitySetDeletedEvent;
import com.dataloom.edm.events.EntitySetMetadataUpdatedEvent;
import com.dataloom.edm.events.PropertyTypesInEntitySetUpdatedEvent;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.linking.Entity;
import com.dataloom.mappers.ObjectMappers;
import com.dataloom.organizations.events.OrganizationCreatedEvent;
import com.dataloom.organizations.events.OrganizationDeletedEvent;
import com.dataloom.organizations.events.OrganizationUpdatedEvent;
import com.dataloom.search.requests.AdvancedSearch;
import com.dataloom.search.requests.SearchResult;
import com.dataloom.search.requests.SearchTerm;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.rpc.RPCIteratorOrdered;

public class SearchService {
    private static final Logger                       logger = LoggerFactory.getLogger( SearchService.class );

    @Inject
    private EventBus                                  eventBus;

    @Inject
    private AuthorizationManager                      authorizations;

    @Inject
    private AbstractSecurableObjectResolveTypeService securableObjectTypes;

    @Inject
    private DatastoreConductorSparkApi                sparkApi;

    @Inject
    private DatastoreConductorElasticsearchApi        elasticsearchApi;

    @Inject
    private HazelcastInstance                         hazelcast;

    @Inject
    private CassandraDataManager                      cdm;

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
    public void deleteEntitySet( EntitySetDeletedEvent event ) {
        elasticsearchApi.deleteEntitySet( event.getEntitySetId() );
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
        elasticsearchApi.createEntityData( event.getEntitySetId(), event.getEntityId(), event.getPropertyValues() );
    }

    public SearchResult executeEntitySetDataSearch(
            UUID entitySetId,
            SearchTerm searchTerm,
            Set<UUID> authorizedProperties ) {
        return elasticsearchApi.executeEntitySetDataSearch( entitySetId,
                searchTerm.getSearchTerm(),
                searchTerm.getStart(),
                searchTerm.getMaxHits(),
                authorizedProperties );
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
            Set<UUID> entitySetIds,
            Map<UUID, Set<String>> fieldSearches,
            int size,
            boolean explain ) {
        return elasticsearchApi.executeEntitySetDataSearchAcrossIndices( entitySetIds, fieldSearches, size, explain );
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
            return elasticsearchApi.executeAdvancedEntitySetDataSearch( entitySetId,
                    authorizedSearches,
                    search.getStart(),
                    search.getMaxHits(),
                    authorizedProperties );
        }

        return new SearchResult( 0, Lists.newArrayList() );
    }

    public List<SetMultimap<UUID, Object>> getTopUtilizers(
            UUID entitySetId,
            UUID propertyTypeId,
            int maxHits,
            Map<UUID, PropertyType> propertyTypes ) {
        UUID requestId = sparkApi.getTopUtilizers( entitySetId, propertyTypeId, propertyTypes );
//        RPCIteratorOrdered<byte[]> it = new RPCIteratorOrdered<byte[]>(
//                requestId,
//                hazelcast.getMap( HazelcastMap.RPC_DATA_ORDERED.name() ) );
        List<SetMultimap<UUID, Object>> results = Lists.newArrayList();
//        TypeReference<List<SetMultimap<UUID, Object>>> resultType = new TypeReference<List<SetMultimap<UUID, Object>>>() {};
        // for ( int i = 0; i < maxHits; i++ ) {
        // if (it.hasNext()) {
        // try {
        // SetMultimap<UUID, Object> row = ObjectMappers.getSmileMapper().readValue( it.next(), resultType );
        // } catch ( IOException e ) {
        // logger.debug( "unable to read row data" );
        // }
        // }
        // }

        return results;
    }

    public List<SetMultimap<UUID, Object>> readTopUtilizers( UUID requestId, int maxHits ) {
        List<SetMultimap<UUID, Object>> results = Lists.newArrayList();
        // RPCIteratorOrdered<byte[]> it = new RPCIteratorOrdered<byte[]>(
        // requestId,
        // hazelcast.getMap( HazelcastMap.RPC_DATA_ORDERED.name() ) );
        List<byte[]> byteResults = cdm.readNumRPCRows( requestId, maxHits );
        TypeReference<SetMultimap<UUID, Object>> resultType = new TypeReference<SetMultimap<UUID, Object>>() {};
        for ( byte[] byteEntity : byteResults ) {
            try {
                SetMultimap<UUID, Object> row = ObjectMappers.getSmileMapper().readValue( byteEntity, resultType );
                results.add( row );
            } catch ( IOException e ) {
            }
        }
        // for ( int i = 0; i < maxHits; i++ ) {
        // if ( it.hasNext() ) {
        // try {
        // SetMultimap<UUID, Object> row = ObjectMappers.getSmileMapper().readValue( it.next(), resultType );
        // } catch ( IOException e ) {
        // logger.debug( "unable to read row data" );
        // }
        // }
        // }
        return results;
    }

}
