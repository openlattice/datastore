package com.dataloom.datastore.search.controllers;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
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
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.mappers.ObjectMappers;
import com.dataloom.search.SearchApi;
import com.dataloom.search.requests.SearchRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.kryptnostic.datastore.exceptions.BadRequestException;
import com.kryptnostic.datastore.services.EdmService;

@RestController
@RequestMapping( SearchApi.CONTROLLER )
public class SearchController implements SearchApi, AuthorizingComponent {

    @Inject
    private SearchService searchService;

    @Inject
    private EdmService edm;

    @Inject
    private AuditQueryService aqs;

    @Inject
    private AuthorizationManager authorizations;
    
    @Inject
    private EdmAuthorizationHelper authorizationsHelper;

    @RequestMapping(
            path = { "/", "" },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    public String executeQueryJson( @RequestBody SearchRequest request ) {
        if ( !request.getOptionalKeyword().isPresent() && !request.getOptionalEntityType().isPresent()
                && !request.getOptionalPropertyTypes().isPresent() ) {
            throw new BadRequestException(
                    "You must specify at least one request body param (keyword 'kw', entity type id 'eid', or property type ids 'pid'" );
        }

        try {
            return ObjectMappers.getJsonMapper().writeValueAsString( searchService
                    .executeEntitySetKeywordSearchQuery( request.getOptionalKeyword(),
                            request.getOptionalEntityType(),
                            request.getOptionalPropertyTypes() ) );
        } catch ( JsonProcessingException e ) {
            e.printStackTrace();
        }
        return Lists.newArrayList().toString();
    }

    @RequestMapping(
            path = { SEARCH_JAVA },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    public Iterable<Map<String, Object>> executeQuery( @RequestBody SearchRequest request ) {
        if ( !request.getOptionalKeyword().isPresent() && !request.getOptionalEntityType().isPresent()
                && !request.getOptionalPropertyTypes().isPresent() ) {
            throw new BadRequestException(
                    "You must specify at least one request body param (keyword 'kw', entity type id 'eid', or property type ids 'pid'" );
        }
        return searchService.executeEntitySetKeywordSearchQuery(
                request.getOptionalKeyword(),
                request.getOptionalEntityType(),
                request.getOptionalPropertyTypes() );
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

    @Override public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

    @RequestMapping(
            path = { ORGANIZATIONS },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public String executeOrganizationSearch( @RequestBody String searchTerm ) {
        try {
            return ObjectMappers.getJsonMapper().writeValueAsString( searchService.executeOrganizationKeywordSearch( searchTerm ) );
        } catch ( JsonProcessingException e ) {
            e.printStackTrace();
            return "[]";
        }
    }
    
    @RequestMapping(
            path = { ENTITY_SET_ID_PATH },
            method = RequestMethod.POST,
            produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public List<Map<String, Object>> executeEntitySetDataQuery(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody String searchTerm ) {
        if ( authorizations.checkIfHasPermissions( ImmutableList.of( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            Set<UUID> authorizedProperties = authorizationsHelper.getAuthorizedPropertiesOnEntitySet( entitySetId,
                    EnumSet.of( Permission.READ ) );
            return searchService.executeEntitySetDataSearch( entitySetId, searchTerm, authorizedProperties );
        }
        return Lists.newArrayList();
    }
}
