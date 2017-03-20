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

package com.dataloom.datastore.search.controllers;

import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.dataloom.auditing.AuditMetric;
import com.dataloom.auditing.AuditQueryService;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.authorization.EdmAuthorizationHelper;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principals;
import com.dataloom.datastore.services.SearchService;
import com.dataloom.edm.EntitySet;
import com.dataloom.search.SearchApi;
import com.dataloom.search.requests.AdvancedSearch;
import com.dataloom.search.requests.LinkingTypeSearch;
import com.dataloom.search.requests.Search;
import com.dataloom.search.requests.SearchResult;
import com.dataloom.search.requests.SearchTerm;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.kryptnostic.datastore.services.EdmService;

@RestController
@RequestMapping( SearchApi.CONTROLLER )
public class SearchController implements SearchApi, AuthorizingComponent {

    @Inject
    private SearchService          searchService;

    @Inject
    private EdmService             edm;

    @Inject
    private AuditQueryService      aqs;

    @Inject
    private AuthorizationManager   authorizations;

    @Inject
    private EdmAuthorizationHelper authorizationsHelper;

    @RequestMapping(
        path = { "/", "" },
        method = RequestMethod.POST,
        produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public SearchResult executeEntitySetKeywordQuery(
            @RequestBody Search search ) {
        if ( !search.getOptionalKeyword().isPresent() && !search.getOptionalEntityType().isPresent()
                && !search.getOptionalPropertyTypes().isPresent() ) {
            throw new IllegalArgumentException(
                    "Your search cannot be empty--you must include at least one of of the three params: keyword ('kw'), entity type id ('eid'), or property type ids ('pid')" );
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
    public Iterable<EntitySet> getPopularEntitySet() {
        Set<EntitySet> entitySets = aqs.getTop100()
                .map( AuditMetric::getAclKey )
                .filter( aclKey -> aclKey.size() == 1 )
                .map( aclKey -> edm.getEntitySet( aclKey.get( 0 ) ) )
                .filter( es -> es != null && isAuthorizedObject( Permission.READ ).test( es ) )
                .collect( Collectors.toSet() );

        if ( entitySets.size() == 0 ) {
            return edm.getEntitySets();
        }

        return entitySets;
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
    public SearchResult executeOrganizationSearch( @RequestBody SearchTerm searchTerm ) {
        return searchService.executeOrganizationKeywordSearch( searchTerm );
    }

    @RequestMapping(
        path = { ENTITY_SET_ID_PATH },
        method = RequestMethod.POST,
        produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public SearchResult executeEntitySetDataQuery(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody SearchTerm searchTerm ) {
        if ( authorizations.checkIfHasPermissions( ImmutableList.of( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            Set<UUID> authorizedProperties = authorizationsHelper.getAuthorizedPropertiesOnEntitySet( entitySetId,
                    EnumSet.of( Permission.READ ) );
            if ( !authorizedProperties.isEmpty() )
                return searchService.executeEntitySetDataSearch( entitySetId, searchTerm, authorizedProperties );
        }
        return new SearchResult( 0, Lists.newArrayList() );
    }

    @RequestMapping(
        path = { ADVANCED + ENTITY_SET_ID_PATH },
        method = RequestMethod.POST,
        produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public SearchResult executeAdvancedEntitySetDataQuery(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody AdvancedSearch search ) {
        if ( authorizations.checkIfHasPermissions( ImmutableList.of( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            Set<UUID> authorizedProperties = authorizationsHelper.getAuthorizedPropertiesOnEntitySet( entitySetId,
                    EnumSet.of( Permission.READ ) );
            return searchService.executeAdvancedEntitySetDataSearch( entitySetId, search, authorizedProperties );
        }
        return new SearchResult( 0, Lists.newArrayList() );
    }

    @Override
    @RequestMapping(
            path = LINKING_TYPES,
            method = RequestMethod.POST,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public SearchResult executeLinkingTypeSearch( @RequestBody LinkingTypeSearch searchTerm ) {
        if ( !searchTerm.getOptionalProperty().isPresent() && !searchTerm.getOptionalSearchTerm().isPresent()
                && !searchTerm.getOptionalSrc().isPresent() && !searchTerm.getOptionalDest().isPresent() ) {
            throw new IllegalArgumentException(
                    "Your search cannot be empty--you must include at least one of of the four search params: keyword ('kw'), property type id ('property'), source property type id ('src'), or destination property type id ('dest')" );
        }

        return searchService.executeLinkingTypeSearch( searchTerm.getOptionalSearchTerm(),
                searchTerm.getOptionalProperty(),
                searchTerm.getOptionalSrc(),
                searchTerm.getOptionalDest(),
                searchTerm.getStart(),
                searchTerm.getMaxHits() );
    }
}
