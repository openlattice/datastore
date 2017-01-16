package com.kryptnostic.datastore.search.controllers;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpServerErrorException;

import com.dataloom.search.SearchApi;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.kryptnostic.datastore.services.SearchService;

@RestController
public class SearchController implements SearchApi {

    @Inject
    private SearchService searchService;

    @RequestMapping(
        path = { "/" + SEARCH },
        method = RequestMethod.POST,
        produces = { MediaType.APPLICATION_JSON_VALUE } )
    public String executeQueryJson(
            @RequestParam(
                value = KEYWORD,
                required = false ) String query,
            @RequestParam(
                value = ENTITY_TYPE_ID,
                required = false ) UUID entityType,
            @RequestBody Set<UUID> propertyTypes ) {
        if ( query == null && entityType == null && propertyTypes == null ) {
            throw new HttpServerErrorException(
                    HttpStatus.BAD_REQUEST,
                    "You must specify at least one query param (keyword 'kw', entity type id 'eid') or request body (property types)" );
        }
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule( new GuavaModule() );
        mapper.registerModule( new JodaModule() );
        Optional<String> optionalQuery = ( query == null ) ? Optional.absent() : Optional.of( query );
        Optional<UUID> optionalEntityType = ( entityType == null ) ? Optional.absent() : Optional.of( entityType );
        Optional<Set<UUID>> optionalPropertyTypes = ( propertyTypes == null ) ? Optional.absent()
                : Optional.of( propertyTypes );
        try {
            return mapper.writeValueAsString( searchService
                    .executeEntitySetKeywordSearchQuery( optionalQuery, optionalEntityType, optionalPropertyTypes ) );
        } catch ( JsonProcessingException e ) {
            e.printStackTrace();
        }
        return Lists.newArrayList().toString();
    }

    @RequestMapping(
        path = { "/" + SEARCH_JAVA },
        method = RequestMethod.POST )
    public Iterable<Map<String, Object>> executeQuery(
            @RequestParam(
                value = KEYWORD,
                required = false ) String query,
            @RequestParam(
                value = ENTITY_TYPE_ID,
                required = false ) UUID entityType,
            @RequestBody Set<UUID> propertyTypes ) {
        Optional<String> optionalQuery = ( query == null ) ? Optional.absent() : Optional.of( query );
        Optional<UUID> optionalEntityType = ( entityType == null ) ? Optional.absent() : Optional.of( entityType );
        return searchService.executeEntitySetKeywordSearchQuery( optionalQuery,
                optionalEntityType,
                Optional.of( propertyTypes ) );
    }

}
