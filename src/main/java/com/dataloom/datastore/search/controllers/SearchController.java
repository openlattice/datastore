package com.dataloom.datastore.search.controllers;

import com.dataloom.auditing.AuditMetric;
import com.dataloom.auditing.AuditQuerySerivce;
import com.dataloom.datastore.services.SearchService;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.mappers.ObjectMappers;
import com.dataloom.search.SearchApi;
import com.dataloom.search.requests.SearchRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.kryptnostic.datastore.exceptions.BadRequestException;
import com.kryptnostic.datastore.services.EdmService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import retrofit2.http.Body;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping( SearchApi.CONTROLLER )
public class SearchController implements SearchApi {

    @Inject
    private SearchService searchService;

    @Inject
    private EdmService edm;

    @Inject
    private AuditQuerySerivce aqs;

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
    public Iterable<Map<String, Object>> executeQuery( @Body SearchRequest request ) {
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
                .filter( es -> es != null )
                .collect( Collectors.toSet() );

        if ( entitySets.size() == 0 ) {
            return edm.getEntitySets();
        }
        return entitySets;
    }

}
