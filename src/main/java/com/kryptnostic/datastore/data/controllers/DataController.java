package com.kryptnostic.datastore.data.controllers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.*;
import com.kryptnostic.conductor.rpc.*;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;
import com.kryptnostic.datastore.serialization.FullQualifedNameJacksonDeserializer;
import com.kryptnostic.datastore.services.*;
import com.squareup.okhttp.Response;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping( DataApi.CONTROLLER )
public class DataController implements DataApi {

    @Inject
    private DataService dataService;

    final String MEDIA_TYPE_CSV = "text/csv";

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
            method = RequestMethod.PUT,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<UUID> getEntitySetOfType( @RequestBody FullQualifiedName fqn ) {
        return dataService.loadEntitySetOfType( fqn );
    }

    @Override
    @RequestMapping(
            path = DataApi.ENTITYSET + DataApi.FULLQUALIFIEDNAME_PATH_WITH_DOT,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<UUID> getEntitySetOfType( @PathVariable( FULLQUALIFIEDNAME ) String fqnString ) {
        return dataService.loadEntitySetOfType( new FullQualifiedName( fqnString ) );
    }

    @Override
    @RequestMapping(
            path = DataApi.ENTITYSET + DataApi.NAME_SPACE_PATH + DataApi.NAME_PATH,
            method = RequestMethod.GET,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<UUID> getEntitySetOfType(
            @PathVariable( NAME_SPACE ) String namespace,
            @PathVariable( NAME ) String name ) {
        return dataService.loadEntitySetOfType( new FullQualifiedName( namespace, name ) );
    }

    @Override
    @RequestMapping(
            path = DataApi.ENTITYSET + DataApi.NAME_PATH + DataApi.TYPE_NAME_PATH + DataApi.ENTITY_DATA,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, MEDIA_TYPE_CSV } )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfEntitySet(
            @PathVariable( NAME ) String entitySetName,
            @PathVariable( TYPE_NAME ) String entityTypeName ) {
        return dataService.getAllEntitiesOfEntitySet( entitySetName, entityTypeName );
    }

    @Override
    @RequestMapping(
            path = DataApi.ENTITYSET + DataApi.FILTERED,
            method = RequestMethod.GET,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = { MediaType.APPLICATION_JSON_VALUE, MEDIA_TYPE_CSV } )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<UUID> getFilteredEntitySet( LookupEntitySetRequest lookupEntitiesRequest ) {
        return null;
    }

    @Override
    @RequestMapping(
            path = DataApi.ENTITY_DATA,
            method = RequestMethod.PUT,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = { MediaType.APPLICATION_JSON_VALUE, MEDIA_TYPE_CSV } )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType( @RequestBody FullQualifiedName fqn ) {
        return dataService.readAllEntitiesOfType( fqn );
    }

    @Override
    @RequestMapping(
            path = DataApi.ENTITY_DATA + DataApi.MULTIPLE,
            method = RequestMethod.PUT,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Iterable<Multimap<FullQualifiedName, Object>>> getAllEntitiesOfTypes(
            @RequestBody
                    List<FullQualifiedName> fqns ) {
        return dataService.readAllEntitiesOfSchema( fqns );
    }

    @Override
    @RequestMapping(
            path = DataApi.ENTITY_DATA + DataApi.FULLQUALIFIEDNAME_PATH_WITH_DOT,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, MEDIA_TYPE_CSV } )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType(
            @PathVariable( FULLQUALIFIEDNAME ) String fanAsString ) {
        return dataService.readAllEntitiesOfType( new FullQualifiedName( fanAsString ) );
    }

    @Override
    @RequestMapping(
            path = DataApi.ENTITY_DATA + DataApi.NAME_SPACE_PATH + DataApi.NAME_PATH,
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, MEDIA_TYPE_CSV } )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType(
            @PathVariable( NAME_SPACE ) String namespace, @PathVariable( NAME ) String name ) {
        return dataService.readAllEntitiesOfType( new FullQualifiedName( namespace, name ) );
    }
    
    @Override
    @RequestMapping(
            path = DataApi.ENTITY_DATA + DataApi.FILTERED,
            method = RequestMethod.PUT,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<UUID> getFilteredEntities( @RequestBody LookupEntitiesRequest lookupEntitiesRequest ) {
	        return dataService.getFilteredEntities( lookupEntitiesRequest );  	
    }
        
    @Override
    @RequestMapping(
            path = DataApi.ENTITY_DATA,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response createEntityData( @RequestBody CreateEntityRequest createEntityRequest ) {
        dataService.createEntityData( createEntityRequest );
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
