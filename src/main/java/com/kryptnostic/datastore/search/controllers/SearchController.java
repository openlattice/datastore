package com.kryptnostic.datastore.search.controllers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import com.dataloom.search.SearchApi;
import com.google.common.base.Optional;
import com.kryptnostic.datastore.services.SearchService;

public class SearchController implements SearchApi {
	
	@Inject
	private SearchService searchService;

	@Override
	public String executeQueryJson(String query, UUID entityType, Set<UUID> propertyTypes) {
		return searchService.executeEntitySetKeywordSearchQuery( query, Optional.of( entityType ), Optional.of( propertyTypes ) ).toString();
	}

	@Override
	public List<Map<String, Object>> executeQuery(String query, UUID entityType, Set<UUID> propertyTypes) {
		return searchService.executeEntitySetKeywordSearchQuery( query, Optional.of( entityType ), Optional.of( propertyTypes ) );
	}

}
