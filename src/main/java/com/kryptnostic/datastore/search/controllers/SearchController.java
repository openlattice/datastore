package com.kryptnostic.datastore.search.controllers;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
    		method = RequestMethod.GET,
    		produces = { MediaType.APPLICATION_JSON_VALUE } )
	public String executeQueryJson(
			@RequestParam(
					value = KEYWORD,
					required = true ) String query,
			@RequestParam(
					value = ENTITY_TYPE_ID,
					required = false ) UUID entityType,
			@RequestParam(
					value = PROPERTY_TYPE_ID,
					required = false ) Set<UUID> propertyTypes) {
		ObjectMapper mapper = new ObjectMapper();
		mapper.registerModule( new GuavaModule() );
		mapper.registerModule( new JodaModule() );
		Optional<UUID> optionalEntityType = ( entityType == null ) ? Optional.absent() : Optional.of( entityType );
		Optional<Set<UUID>> optionalPropertyTypes = ( propertyTypes == null ) ? Optional.absent() : Optional.of( propertyTypes );
		try {
			return mapper.writeValueAsString( searchService
					.executeEntitySetKeywordSearchQuery( query, optionalEntityType, optionalPropertyTypes ) );
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return Lists.newArrayList().toString();
	}
    

    @RequestMapping(
    		path = { "/" + SEARCH },
    		method = RequestMethod.POST )
	public Iterable<Map<String, Object>> executeQuery(
			@RequestParam(
					value = KEYWORD,
					required = true ) String query,
			@RequestParam(
					value = ENTITY_TYPE_ID,
					required = false ) UUID entityType,
			@RequestBody Set<UUID> propertyTypes ) {
    	Optional<UUID> optionalEntityType = ( entityType == null ) ? Optional.absent() : Optional.of( entityType );
		return searchService.executeEntitySetKeywordSearchQuery( query, optionalEntityType, Optional.of( propertyTypes ) );
	}

}
