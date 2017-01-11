package com.kryptnostic.datastore.search.controllers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import com.dataloom.search.SearchApi;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Optional;
import com.kryptnostic.datastore.services.SearchService;

public class SearchController implements SearchApi {
	
	@Inject
	private SearchService searchService;

	@Override
	public String executeQueryJson(String query, UUID entityType, Set<UUID> propertyTypes) {
		ObjectMapper mapper = new ObjectMapper();
		mapper.registerModule( new GuavaModule() );
		mapper.registerModule( new JodaModule() );
		try {
			return mapper.writeValueAsString( searchService
					.executeEntitySetKeywordSearchQuery( query, Optional.of( entityType ), Optional.of( propertyTypes ) ) );
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return "";
	}

	@Override
	public List<Map<String, Object>> executeQuery(String query, UUID entityType, Set<UUID> propertyTypes) {
		return searchService.executeEntitySetKeywordSearchQuery( query, Optional.of( entityType ), Optional.of( propertyTypes ) );
	}

}
