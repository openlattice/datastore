package com.kryptnostic.datastore.data.controllers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
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
import retrofit.http.Body;
import retrofit.http.Path;

@RestController
@RequestMapping( DataApi.CONTROLLER )
public class DataController implements DataApi {
    @Inject
    private EdmManager modelService;

    @Inject
    private DataService dataService;

    @RequestMapping(
            path = { "/object/{id}" },
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, Object> getObject( @PathVariable( "id" ) UUID objectId ) {
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
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType(
            LoadAllEntitiesOfTypeRequest loadAllEntitiesOfTypeRequest ) {
        return null;
    }

    @Override
    @RequestMapping(
            path = DataApi.ENTITY_DATA,
            method = RequestMethod.PUT,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<SetMultimap<FullQualifiedName, Object>> getAllEntitiesOfType( @RequestBody FullQualifiedName fqn ) {
        return dataService.readAllEntitiesOfType( fqn );
    }

    @Override
    @RequestMapping(
            path = DataApi.ENTITY_DATA + DataApi.FULLQUALIFIEDNAME_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<SetMultimap<FullQualifiedName, Object>> getAllEntitiesOfType(
            @PathVariable( FULLQUALIFIEDNAME ) String fanAsString ) {
        return dataService.readAllEntitiesOfType( new FullQualifiedName( fanAsString ) );
    }

    @Override
    @RequestMapping(
            path = DataApi.ENTITY_DATA + DataApi.NAME_SPACE_PATH + DataApi.NAME_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<SetMultimap<FullQualifiedName, Object>> getAllEntitiesOfType(
            @PathVariable( NAME_SPACE ) String namespace, @PathVariable( NAME ) String name ) {
        return dataService.readAllEntitiesOfType( new FullQualifiedName( namespace, name ) );
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

    @Override
    @RequestMapping(
            path = "/multimap",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public SetMultimap<FullQualifiedName, Object> getSetMultimap() {
        SetMultimap<FullQualifiedName, Object> test = HashMultimap.create();
        test.put( new FullQualifiedName( "test", "k1" ), "hallo" );
        test.put( new FullQualifiedName( "test", "k1" ), "world" );
        test.put( new FullQualifiedName( "test", "k2" ), "gogogo" );
        test.put( new FullQualifiedName( "test2", "k2" ), "haha" );
        return test;
    }

    @Override
    @RequestMapping(
            path = "/multimap",
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response createSetMultimap(
            @Body SetMultimap<FullQualifiedName, Object> setMultimap ) {

        return null;
    }

}
