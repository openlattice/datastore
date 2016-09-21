package com.kryptnostic.datastore.data.controllers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import com.google.common.collect.SetMultimap;
import com.kryptnostic.conductor.rpc.*;
import com.kryptnostic.datastore.services.DataApi;
import com.squareup.okhttp.Response;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import com.kryptnostic.datastore.services.DataService;
import com.kryptnostic.datastore.services.EdmManager;

@RestController
@RequestMapping( DataApi.CONTROLLER )
public class DataController implements DataApi {
    @Inject
    private EdmManager          modelService;

    @Inject
    private DataService dataService;

    @RequestMapping(
        path = { "/object/{id}" },
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, Object> getObject( @PathVariable("id") UUID objectId) {
        return dataService.getObject( objectId );
    }

    @Override
    @RequestMapping(
            path = DataApi.ENTITYSET,
            method = RequestMethod.GET,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public List<UUID> getAllEntitySet( LoadEntitySetRequest loadEntitySetRequest ) {
        return null;
    }

    @Override
    @RequestMapping(
            path = DataApi.ENTITYSET + DataApi.FILTERED,
            method = RequestMethod.GET,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public List<UUID> getFilteredEntitySet( LookupEntitySetRequest lookupEntitiesRequest ) {
        return null;
    }

    @Override
    @RequestMapping(
            path = DataApi.ENTITY_DATA,
            method = RequestMethod.GET,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<SetMultimap<FullQualifiedName, Object>> getAllEntitiesOfType(
            LoadAllEntitiesOfTypeRequest loadAllEntitiesOfTypeRequest ) {
        return null;
    }

    @Override
    @RequestMapping(
            path = DataApi.ENTITY_DATA + DataApi.FILTERED,
            method = RequestMethod.GET,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<SetMultimap<FullQualifiedName, Object>> getFilteredEntitiesOfType( LookupEntitiesRequest lookupEntitiesRequest ) {
        return null;
    }

    @Override
    @RequestMapping(
            path = DataApi.ENTITY_DATA,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response createEntityData( CreateEntityRequest createEntityRequest ) {
        return null;
    }

    @Override
    @RequestMapping(
            path = DataApi.INTEGRATION,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, String> getAllIntegrationScripts() {
        return dataService.getAllIntegrationScripts();
    }

    @Override
    @RequestMapping(
            path = DataApi.INTEGRATION,
            method = RequestMethod.PUT,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, String> getIntegrationScript( @RequestBody Set<String> urls ) {
        return dataService.getIntegrationScriptForUrl( urls );
    }

    @Override
    @RequestMapping(
            path = DataApi.INTEGRATION,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response createIntegrationScript( @RequestBody Map<String, String> integrationScripts ) {
        dataService.createIntegrationScript( integrationScripts );
        return null;
    }
}
