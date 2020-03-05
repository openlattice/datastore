/*
 * Copyright (C) 2018. OpenLattice, Inc.
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
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.datastore.search.controllers;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.*;
import com.openlattice.auditing.AuditEventType;
import com.openlattice.auditing.AuditableEvent;
import com.openlattice.auditing.AuditingComponent;
import com.openlattice.auditing.AuditingManager;
import com.openlattice.authorization.*;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.authorization.util.AuthorizationUtils;
import com.openlattice.data.requests.NeighborEntityDetails;
import com.openlattice.data.requests.NeighborEntityIds;
import com.openlattice.datastore.apps.services.AppService;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.EntitySetManager;
import com.openlattice.edm.EntitySet;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.Organization;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.search.SearchApi;
import com.openlattice.search.SearchService;
import com.openlattice.search.SortDefinition;
import com.openlattice.search.graph.SearchGraphService;
import com.openlattice.search.requests.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.jetbrains.annotations.NotNull;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.openlattice.authorization.EdmAuthorizationHelper.READ_PERMISSION;

@SuppressFBWarnings(
        value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE",
        justification = "NPEs are prevented by Preconditions.checkState but SpotBugs doesn't understand this" )
@RestController
@RequestMapping( SearchApi.CONTROLLER )
public class SearchController implements SearchApi, AuthorizingComponent, AuditingComponent {

    @Inject
    private SearchService searchService;

    @Inject
    private SearchGraphService searchGraphService;

    @Inject
    private EdmService edm;

    @Inject
    private EntitySetManager entitySetManager;

    @Inject
    private AppService appService;

    @Inject
    private AuthorizationManager authorizations;

    @Inject
    private EdmAuthorizationHelper authorizationsHelper;

    @Inject
    private HazelcastOrganizationService organizationService;

    @Inject
    private SecurePrincipalsManager spm;

    @Inject
    private AuditingManager auditingManager;


    @RequestMapping(
            path = { "/", "" },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public SearchResult executeEntitySetKeywordQuery(
            @RequestBody Search search ) {
        if ( search.getOptionalKeyword().isEmpty() && search.getOptionalEntityType().isEmpty()
                && search.getOptionalPropertyTypes().isEmpty() ) {
            throw new IllegalArgumentException(
                    "Your search cannot be empty--you must include at least one of of the three params: keyword " +
                            "('kw'), entity type id ('eid'), or property type ids ('pid')" );
        }
        return searchService
                .executeEntitySetKeywordSearchQuery( search.getOptionalKeyword(),
                        search.getOptionalEntityType(),
                        search.getOptionalPropertyTypes(),
                        search.getStart(),
                        search.getMaxHits() );
    }

    @RequestMapping(
            path = { POPULAR },
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public Iterable<EntitySet> getPopularEntitySet() {
        return entitySetManager.getEntitySets();
    }

    @RequestMapping(
            path = { ENTITY_SETS + START_PATH + NUM_RESULTS_PATH },
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public SearchResult getEntitySets(
            @PathVariable( START ) int start,
            @PathVariable( NUM_RESULTS ) int maxHits ) {
        return searchService
                .executeEntitySetKeywordSearchQuery( Optional.of( "*" ),
                        Optional.empty(),
                        Optional.empty(),
                        start,
                        Math.min( maxHits, SearchApi.MAX_SEARCH_RESULTS ) );
    }

    @RequestMapping(
            path = { "/", "" },
            method = RequestMethod.PATCH,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public DataSearchResult searchEntitySetData( @RequestBody SearchConstraints searchConstraints ) {

        validateSearch( searchConstraints );

        // check read on entity sets
        final var authorizedEntitySetIds = authorizationsHelper
                .getAuthorizedEntitySets( Set.of( searchConstraints.getEntitySetIds() ), READ_PERMISSION );

        DataSearchResult results = new DataSearchResult( 0, Lists.newArrayList() );

        // if user has read access on all normal entity sets
        if ( authorizedEntitySetIds.size() == searchConstraints.getEntitySetIds().length ) {
            final var authorizedPropertyTypesByEntitySet = authorizationsHelper.getAuthorizedPropertiesOnEntitySets(
                    authorizedEntitySetIds, READ_PERMISSION, Principals.getCurrentPrincipals() );

            results = searchService.executeSearch( searchConstraints, authorizedPropertyTypesByEntitySet );
        }

        List<AuditableEvent> searchEvents = Lists.newArrayList();
        for ( int i = 0; i < searchConstraints.getEntitySetIds().length; i++ ) {
            searchEvents.add( new AuditableEvent(
                    spm.getCurrentUserId(),
                    new AclKey( searchConstraints.getEntitySetIds()[ i ] ),
                    AuditEventType.SEARCH_ENTITY_SET_DATA,
                    "Entity set data searched through SearchApi.searchEntitySetData",
                    Optional.of( getEntityKeyIdsFromSearchResult( results ) ),
                    ImmutableMap.of( "query", searchConstraints ),
                    OffsetDateTime.now(),
                    Optional.empty()
            ) );
        }

        recordEvents( searchEvents );

        return results;
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

    @RequestMapping(
            path = { ORGANIZATIONS },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public SearchResult executeOrganizationSearch( @RequestBody SearchTerm searchTerm ) {
        return searchService.executeOrganizationKeywordSearch( searchTerm );
    }

    @RequestMapping(
            path = { ENTITY_SET_ID_PATH },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public DataSearchResult executeEntitySetDataQuery(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody SearchTerm searchTerm ) {

        return searchEntitySetData(
                SearchConstraints.simpleSearchConstraints(
                        new UUID[]{ entitySetId },
                        searchTerm.getStart(),
                        searchTerm.getMaxHits(),
                        searchTerm.getSearchTerm(),
                        searchTerm.getFuzzy() )
        );
    }

    @RequestMapping(
            path = { ADVANCED + ENTITY_SET_ID_PATH },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public DataSearchResult executeAdvancedEntitySetDataQuery(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody AdvancedSearch search ) {

        return searchEntitySetData(
                SearchConstraints.advancedSearchConstraints(
                        new UUID[]{ entitySetId },
                        search.getStart(),
                        search.getMaxHits(),
                        search.getSearches() )
        );
    }

    @RequestMapping(
            path = { ENTITY_TYPES },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public SearchResult executeEntityTypeSearch( @RequestBody SearchTerm searchTerm ) {
        return searchService.executeEntityTypeSearch( searchTerm.getSearchTerm(),
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @RequestMapping(
            path = { ASSOCIATION_TYPES },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public SearchResult executeAssociationTypeSearch( @RequestBody SearchTerm searchTerm ) {
        return searchService.executeAssociationTypeSearch( searchTerm.getSearchTerm(),
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @RequestMapping(
            path = { PROPERTY_TYPES },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public SearchResult executePropertyTypeSearch( @RequestBody SearchTerm searchTerm ) {
        return searchService.executePropertyTypeSearch( searchTerm.getSearchTerm(),
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @RequestMapping(
            path = { APP },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public SearchResult executeAppSearch( @RequestBody SearchTerm searchTerm ) {
        return searchService.executeAppSearch( searchTerm.getSearchTerm(),
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @RequestMapping(
            path = { ENTITY_TYPES + COLLECTIONS },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public SearchResult executeEntityTypeCollectionSearch( @RequestBody SearchTerm searchTerm ) {
        return searchService.executeEntityTypeCollectionSearch( searchTerm.getSearchTerm(),
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @RequestMapping(
            path = { ENTITY_SETS + COLLECTIONS },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public SearchResult executeEntitySetCollectionSearch( @RequestBody SearchTerm searchTerm ) {
        return searchService.executeEntitySetCollectionQuery( searchTerm.getSearchTerm(),
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @RequestMapping(
            path = { ENTITY_TYPES + FQN },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public SearchResult executeFQNEntityTypeSearch( @RequestBody FQNSearchTerm searchTerm ) {
        return searchService.executeFQNEntityTypeSearch( searchTerm.getNamespace(),
                searchTerm.getName(),
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @RequestMapping(
            path = { PROPERTY_TYPES + FQN },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public SearchResult executeFQNPropertyTypeSearch( @RequestBody FQNSearchTerm searchTerm ) {
        return searchService.executeFQNPropertyTypeSearch( searchTerm.getNamespace(),
                searchTerm.getName(),
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }

    @RequestMapping(
            path = { ENTITY_SET_ID_PATH + ENTITY_KEY_ID_PATH },
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public List<NeighborEntityDetails> executeEntityNeighborSearch(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId ) {
        List<NeighborEntityDetails> neighbors = Lists.newArrayList();

        Set<Principal> principals = Principals.getCurrentPrincipals();
        if ( getEntitySetsForRead( Set.of( entitySetId ), principals ) != null ) {
            neighbors = searchGraphService.executeEntityNeighborSearch(
                    new EntityNeighborsFilterBulk( Map.of( entitySetId, ImmutableSet.of( entityKeyId ) ) ),
                    principals
            ).getOrDefault( entityKeyId, ImmutableList.of() );
        }

        UUID userId = spm.getCurrentUserId();

        Map<UUID, Set<UUID>> neighborsByEntitySet = Maps
                .newHashMapWithExpectedSize( ( int ) ( 1.5 * neighbors.size() ) );
        neighbors.forEach( neighborEntityDetails -> {
            var associationEsId = neighborEntityDetails.getAssociationEntitySet().getId();
            neighborsByEntitySet.putIfAbsent( associationEsId, Sets.newHashSet() );
            neighborsByEntitySet.get( associationEsId )
                    .add( getEntityKeyId( neighborEntityDetails.getAssociationDetails() ) );

            if ( neighborEntityDetails.getNeighborEntitySet().isPresent() ) {
                var neighborEsId = neighborEntityDetails.getNeighborEntitySet().get().getId();
                neighborsByEntitySet.putIfAbsent( neighborEsId, Sets.newHashSet() );
                neighborsByEntitySet.get( neighborEsId )
                        .add( getEntityKeyId( neighborEntityDetails.getNeighborDetails().get() ) );
            }
        } );

        List<AuditableEvent> events = new ArrayList<>( neighborsByEntitySet.keySet().size() + 1 );
        events.add( new AuditableEvent(
                userId,
                new AclKey( entitySetId ),
                AuditEventType.LOAD_ENTITY_NEIGHBORS,
                "Load entity neighbors through SearchApi.executeEntityNeighborSearch",
                Optional.of( ImmutableSet.of( entityKeyId ) ),
                ImmutableMap.of(),
                OffsetDateTime.now(),
                Optional.empty()
        ) );

        for ( UUID neighborEntitySetId : neighborsByEntitySet.keySet() ) {
            events.add( new AuditableEvent(
                    userId,
                    new AclKey( neighborEntitySetId ),
                    AuditEventType.READ_ENTITIES,
                    "Read entities as neighbors through SearchApi.executeEntityNeighborSearch",
                    Optional.of( neighborsByEntitySet.get( neighborEntitySetId ) ),
                    ImmutableMap.of( "aclKeySearched", new AclKey( entitySetId, entityKeyId ) ),
                    OffsetDateTime.now(),
                    Optional.empty()
            ) );
        }

        recordEvents( events );

        return neighbors;
    }

    @RequestMapping(
            path = { ENTITY_SET_ID_PATH + NEIGHBORS },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public Map<UUID, List<NeighborEntityDetails>> executeEntityNeighborSearchBulk(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody Set<UUID> entityKeyIds ) {
        return executeFilteredEntityNeighborSearch( new EntityNeighborsFilterBulk( Map.of( entitySetId,
                entityKeyIds ) ) );
    }

    @RequestMapping(
            path = { NEIGHBORS + ADVANCED },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public Map<UUID, List<NeighborEntityDetails>> executeFilteredEntityNeighborSearch(
            @RequestBody EntityNeighborsFilterBulk filter
    ) {
        Set<Principal> principals = Principals.getCurrentPrincipals();
        var entitySetIds = filter.getEntityKeyIds().keySet();

        Map<UUID, List<NeighborEntityDetails>> result = Maps.newHashMap();
        if ( getEntitySetsForRead( entitySetIds, principals ) != null ) {
            result = searchGraphService.executeEntityNeighborSearch( filter, principals );
        }

        /* audit */

        int expectedSize = ( result.isEmpty() ) ? 0 :
                ( int ) ( result.size() * 1.5 * result.values().iterator().next().size() );
        Map<UUID, List<UUID>> neighborsByEntitySet = Maps.newHashMapWithExpectedSize( expectedSize );

        result.values().forEach( neighborList ->
                neighborList.forEach( neighborEntityDetails -> {
                    var associationEsId = neighborEntityDetails.getAssociationEntitySet().getId();
                    neighborsByEntitySet.putIfAbsent( associationEsId, Lists.newArrayList() );
                    neighborsByEntitySet.get( associationEsId )
                            .add( getEntityKeyId( neighborEntityDetails.getAssociationDetails() ) );

                    if ( neighborEntityDetails.getNeighborEntitySet().isPresent() ) {
                        var neighborEsId = neighborEntityDetails.getNeighborEntitySet().get().getId();
                        neighborsByEntitySet.putIfAbsent( neighborEsId, Lists.newArrayList() );
                        neighborsByEntitySet.get( neighborEsId ).add(
                                getEntityKeyId( neighborEntityDetails.getNeighborDetails().get() ) );
                    }
                } )
        );

        List<AuditableEvent> events = new ArrayList<>(
                neighborsByEntitySet.keySet().size() + filter.getEntityKeyIds().size() );
        UUID userId = spm.getCurrentUserId();

        filter.getEntityKeyIds().forEach( ( entitySetId, entityKeyIds ) -> {
                    int segments = filter.getEntityKeyIds().size() / AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;
                    if ( filter.getEntityKeyIds().size() % AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT != 0 ) {
                        segments++;
                    }

                    List<UUID> entityKeyIdsAsList = Lists.newArrayList( entityKeyIds );

                    for ( int i = 0; i < segments; i++ ) {

                        int fromIndex = i * AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;
                        int toIndex = i == segments - 1 ?
                                entityKeyIdsAsList.size() :
                                ( i + 1 ) * AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;

                        Set<UUID> segmentOfIds = Sets.newHashSet( entityKeyIdsAsList.subList( fromIndex, toIndex ) );

                        events.add( new AuditableEvent(
                                userId,
                                new AclKey( entitySetId ),
                                AuditEventType.LOAD_ENTITY_NEIGHBORS,
                                "Load neighbors of entities with filter through SearchApi" +
                                        ".executeFilteredEntityNeighborSearch",
                                Optional.of( segmentOfIds ),
                                ImmutableMap.of( "filters",
                                        new EntityNeighborsFilterBulk( Map.of( entitySetId, segmentOfIds ),
                                                filter.getSrcEntitySetIds(),
                                                filter.getDstEntitySetIds(),
                                                filter.getAssociationEntitySetIds() ) ),
                                OffsetDateTime.now(),
                                Optional.empty()
                        ) );
                    }
                }
        );

        for ( UUID neighborEntitySetId : neighborsByEntitySet.keySet() ) {
            List<UUID> neighbors = neighborsByEntitySet.get( neighborEntitySetId );

            int neighborSegments = neighbors.size() / AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;
            if ( neighbors.size() % AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT != 0 ) {
                neighborSegments++;
            }

            for ( int i = 0; i < neighborSegments; i++ ) {

                int fromIndex = i * AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;
                int toIndex = i == neighborSegments - 1 ?
                        neighbors.size() :
                        ( i + 1 ) * AuditingComponent.MAX_ENTITY_KEY_IDS_PER_EVENT;

                events.add( new AuditableEvent(
                        userId,
                        new AclKey( neighborEntitySetId ),
                        AuditEventType.READ_ENTITIES,
                        "Read entities as filtered neighbors through SearchApi.executeFilteredEntityNeighborSearch",
                        Optional.of( Sets.newHashSet( neighbors.subList( fromIndex, toIndex ) ) ),
                        ImmutableMap.of( "entitySetIds", entitySetIds ),
                        OffsetDateTime.now(),
                        Optional.empty()
                ) );
            }
        }

        recordEvents( events );

        return result;
    }

    @RequestMapping(
            path = { ENTITY_SET_ID_PATH + NEIGHBORS + ADVANCED },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public Map<UUID, List<NeighborEntityDetails>> executeFilteredEntityNeighborSearch(
            @PathVariable UUID entitySetId,
            @RequestBody EntityNeighborsFilter filter
    ) {
        return executeFilteredEntityNeighborSearch( new EntityNeighborsFilterBulk( entitySetId, filter ) );
    }


    @RequestMapping(
            path = { NEIGHBORS + ADVANCED + IDS },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public Map<UUID, Map<UUID, Map<UUID, Set<NeighborEntityIds>>>> executeFilteredEntityNeighborIdsSearch(
            @RequestBody EntityNeighborsFilterBulk filter ) {
        Set<Principal> principals = Principals.getCurrentPrincipals();

        Map<UUID, Map<UUID, Map<UUID, Set<NeighborEntityIds>>>> result = Maps.newHashMap();
        var entitySets = getEntitySetsForRead( filter.getEntityKeyIds().keySet(), principals );
        if ( entitySets != null ) {
            var groupedEntitySets = entitySets.entrySet().stream()
                    .collect( Collectors.groupingBy( esEntry -> esEntry.getValue().isLinking() ) );
            if ( !groupedEntitySets.get( false ).isEmpty() ) {
                var regularEntitySetIds = groupedEntitySets.get( false ).stream()
                        .map( Map.Entry::getKey ).collect( Collectors.toSet() );
                result = searchGraphService.executeEntityNeighborIdsSearch(
                        new EntityNeighborsFilterBulk(
                                filter.getEntityKeyIds().entrySet().stream()
                                        .filter( entry -> regularEntitySetIds.contains( entry.getKey() ) )
                                        .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) ),
                                filter.getSrcEntitySetIds(),
                                filter.getDstEntitySetIds(),
                                filter.getAssociationEntitySetIds()
                        ),
                        principals );
            } else if ( !groupedEntitySets.get( true ).isEmpty() ) {
                var linkedEntitySetIds = groupedEntitySets.get( true ).stream()
                        .map( Map.Entry::getKey ).collect( Collectors.toSet() );
                result.putAll(
                        searchGraphService.executeLinkingEntityNeighborIdsSearch(
                                groupedEntitySets.get( true ).stream()
                                        .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) ),
                                new EntityNeighborsFilterBulk(
                                        filter.getEntityKeyIds().entrySet().stream()
                                                .filter( entry -> linkedEntitySetIds.contains( entry.getKey() ) )
                                                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) ),
                                        filter.getSrcEntitySetIds(),
                                        filter.getDstEntitySetIds(),
                                        filter.getAssociationEntitySetIds()
                                ),
                                principals ) );
            }
        }


        /* audit */
        Map<UUID, Set<UUID>> neighborsByEntitySet = Maps.newHashMap();

        result.values().forEach( associationMap ->
                associationMap.forEach( ( associationEsId, association ) -> {
                    neighborsByEntitySet.putIfAbsent( associationEsId, Sets.newHashSet() );

                    association.forEach( ( neighborEsId, neighbors ) -> {
                        neighborsByEntitySet.putIfAbsent( neighborEsId, Sets.newHashSet() );
                        neighbors.forEach( neighbor -> {
                                    neighborsByEntitySet.get( associationEsId )
                                            .add( neighbor.getAssociationEntityKeyId() );
                                    neighborsByEntitySet.get( neighborEsId ).add( neighbor.getNeighborEntityKeyId() );
                                }
                        );
                    } );
                } )
        );

        List<AuditableEvent> events = new ArrayList<>( neighborsByEntitySet.size() + filter.getEntityKeyIds().size() );
        UUID userId = spm.getCurrentUserId();
        filter.getEntityKeyIds().forEach( ( entitySetId, entityKeyIds ) ->
                events.add( new AuditableEvent(
                        userId,
                        new AclKey( entitySetId ),
                        AuditEventType.LOAD_ENTITY_NEIGHBORS,
                        "Load neighbors of entities with filter through SearchApi." +
                                "executeFilteredEntityNeighborSearch",
                        Optional.of( entityKeyIds ),
                        ImmutableMap.of( "filters", new EntityNeighborsFilterBulk(
                                Map.of( entitySetId, entityKeyIds ),
                                filter.getSrcEntitySetIds(),
                                filter.getDstEntitySetIds(),
                                filter.getAssociationEntitySetIds()
                        ) ),
                        OffsetDateTime.now(),
                        Optional.empty()
                ) )
        );

        for ( UUID neighborEntitySetId : neighborsByEntitySet.keySet() ) {
            events.add( new AuditableEvent(
                    userId,
                    new AclKey( neighborEntitySetId ),
                    AuditEventType.READ_ENTITIES,
                    "Read entities as filtered neighbors through SearchApi.executeFilteredEntityNeighborIdsSearch",
                    Optional.of( neighborsByEntitySet.get( neighborEntitySetId ) ),
                    ImmutableMap.of( "filter", filter ),
                    OffsetDateTime.now(),
                    Optional.empty()
            ) );
        }

        recordEvents( events );

        return result;
    }

    @RequestMapping(
            path = { ENTITY_SET_ID_PATH + NEIGHBORS + ADVANCED + IDS },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    @Timed
    public Map<UUID, Map<UUID, Map<UUID, Set<NeighborEntityIds>>>> executeFilteredEntityNeighborIdsSearch(
            @PathVariable UUID entitySetId,
            @RequestBody EntityNeighborsFilter filter ) {
        return executeFilteredEntityNeighborIdsSearch( new EntityNeighborsFilterBulk( entitySetId, filter ) );
    }

    @RequestMapping(
            path = { EDM + INDEX },
            method = RequestMethod.GET )
    @Override
    @Timed
    public Void triggerEdmIndex() {
        ensureAdminAccess();
        searchService.triggerEntitySetIndex();
        searchService.triggerPropertyTypeIndex( Lists.newArrayList( edm.getPropertyTypes() ) );
        searchService.triggerEntityTypeIndex( Lists.newArrayList( edm.getEntityTypes() ) );
        searchService.triggerAssociationTypeIndex( Lists.newArrayList( edm.getAssociationTypes() ) );
        searchService.triggerAppIndex( Lists.newArrayList( appService.getApps() ) );
        searchService.triggerAppTypeIndex( Lists.newArrayList( appService.getAppTypes() ) );
        return null;
    }

    @RequestMapping(
            path = { ENTITY_SETS + INDEX + ENTITY_SET_ID_PATH },
            method = RequestMethod.GET )
    @Override
    @Timed
    public Void triggerEntitySetDataIndex( @PathVariable( ENTITY_SET_ID ) UUID entitySetId ) {
        ensureAdminAccess();
        searchService.triggerEntitySetDataIndex( entitySetId );
        return null;
    }

    @RequestMapping(
            path = { ENTITY_SETS + INDEX },
            method = RequestMethod.GET )
    @Override
    @Timed
    public Void triggerAllEntitySetDataIndex() {
        ensureAdminAccess();
        searchService.triggerAllEntitySetDataIndex();
        return null;
    }

    @RequestMapping(
            path = { ORGANIZATIONS + INDEX },
            method = RequestMethod.GET )
    @Override
    @Timed
    public Void triggerAllOrganizationsIndex() {
        ensureAdminAccess();
        List<Organization> allOrganizations = Lists.newArrayList(
                organizationService.getOrganizations(
                        getAccessibleObjects( SecurableObjectType.Organization,
                                EnumSet.of( Permission.READ ) ) // TODO: other access check??
                                .parallel()
                                .filter( Objects::nonNull )
                                .map( AuthorizationUtils::getLastAclKeySafely ) ) );
        searchService.triggerAllOrganizationsIndex( allOrganizations );
        return null;
    }

    @RequestMapping(
            path = { ORGANIZATIONS + INDEX + ORGANIZATION_ID_PATH },
            method = RequestMethod.GET )
    @Override
    @Timed
    public Void triggerOrganizationIndex( @PathVariable( ORGANIZATION_ID ) UUID organizationId ) {
        ensureAdminAccess();
        searchService.triggerOrganizationIndex( organizationService.getOrganization( organizationId ) );
        return null;
    }

    @NotNull
    @Override
    public AuditingManager getAuditingManager() {
        return auditingManager;
    }

    private static Set<UUID> getEntityKeyIdsFromSearchResult( DataSearchResult searchResult ) {
        return searchResult.getHits().stream().map( SearchController::getEntityKeyId ).collect( Collectors.toSet() );
    }

    private static UUID getEntityKeyId( Map<FullQualifiedName, Set<Object>> entity ) {
        return SearchService.getEntityKeyId( entity );
    }

    /**
     * Checks read permission on requested entity sets and their normal entity sets.
     *
     * @param entitySetIds A non-empty set of entity set ids.
     * @param principals   Set of principals to test read permission against.
     * @return The authorized set of entity sets mapped by their ids or null, if the check failed.
     */
    private Map<UUID, EntitySet> getEntitySetsForRead( Set<UUID> entitySetIds, Set<Principal> principals ) {
        if ( authorizationsHelper
                .getAuthorizedEntitySetsForPrincipals( entitySetIds, EnumSet.of( Permission.READ ), principals ).size()
                == entitySetIds.size() ) {

            var entitySets = entitySetManager.getEntitySetsAsMap( entitySetIds );
            checkState(
                    entitySets.size() == entitySetIds.size(),
                    "Could not find one or more entity sets with ids: {}",
                    entitySetIds
            );

            // do authorization checks on normal entity set ids too
            final var normalEntitySetIds = entitySets.values().stream()
                    .filter( EntitySet::isLinking )
                    .flatMap( es -> es.getLinkedEntitySets().stream() ).collect( Collectors.toSet() );
            final var authorizedEntitySets = authorizationsHelper.getAuthorizedEntitySetsForPrincipals(
                    normalEntitySetIds, EnumSet.of( Permission.READ ), principals
            );

            if ( authorizedEntitySets.size() != normalEntitySetIds.size() ) {
                logger.warn(
                        "Read authorization failed some of the normal entity sets {} of linking entity set(s).",
                        normalEntitySetIds );
                return null;
            }
            return entitySets;
        } else {
            logger.warn( "Read authorization failed some entity sets with ids: {}", entitySetIds );
            return null;
        }
    }

    private void validateSearch( SearchConstraints searchConstraints ) {

        /* Check sort is valid */
        SortDefinition sort = searchConstraints.getSortDefinition();
        switch ( sort.getSortType() ) {

            case field:
            case geoDistance: {
                UUID sortPropertyTypeId = sort.getPropertyTypeId();
                EdmPrimitiveTypeKind datatype = edm.getPropertyType( sortPropertyTypeId ).getDatatype();
                Set<EdmPrimitiveTypeKind> allowedDatatypes = sort.getSortType().getAllowedDatatypes();
                if ( !allowedDatatypes.contains( datatype ) ) {
                    throw new IllegalArgumentException(
                            "SortType " + sort.getSortType() + " cannot be executed on property type "
                                    + sortPropertyTypeId
                                    + " because it is of datatype " + datatype + ". Allowed datatypes are: "
                                    + allowedDatatypes );
                }
            }
        }
    }

}
