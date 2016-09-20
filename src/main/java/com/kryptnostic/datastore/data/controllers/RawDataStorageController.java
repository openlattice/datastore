package com.kryptnostic.datastore.data.controllers;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import com.google.common.collect.SetMultimap;
import com.kryptnostic.conductor.rpc.*;
import com.kryptnostic.datastore.services.RawDataStorageApi;
import com.squareup.okhttp.Response;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import com.kryptnostic.datastore.services.RawDataStorageService;
import com.kryptnostic.datastore.services.EdmManager;

@RestController
@RequestMapping( RawDataStorageApi.CONTROLLER )
public class RawDataStorageController implements RawDataStorageApi {
    @Inject
    private EdmManager          modelService;

    @Inject
    private RawDataStorageService storage;

    @RequestMapping(
        path = { "/object/{id}" },
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, Object> getObject( @PathVariable("id") UUID objectId) {
        return storage.getObject( objectId );
    }

    @Override
    @RequestMapping(
            path = RawDataStorageApi.ENTITYSET,
            method = RequestMethod.GET,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public List<UUID> getAllEntitySet( LoadEntitySetRequest loadEntitySetRequest ) {
        return null;
    }

    @Override
    @RequestMapping(
            path = RawDataStorageApi.ENTITYSET + RawDataStorageApi.FILTERED,
            method = RequestMethod.GET,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public List<UUID> getFilteredEntitySet( LookupEntitySetRequest lookupEntitiesRequest ) {
        return null;
    }

    @Override
    @RequestMapping(
            path = RawDataStorageApi.ENTITY_DATA,
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
            path = RawDataStorageApi.ENTITY_DATA + RawDataStorageApi.FILTERED,
            method = RequestMethod.GET,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<SetMultimap<FullQualifiedName, Object>> getFilteredEntitiesOfType( LookupEntitiesRequest lookupEntitiesRequest ) {
        return null;
    }

    @Override
    @RequestMapping(
            path = RawDataStorageApi.ENTITY_DATA,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response createEntityData( CreateEntityRequest createEntityRequest ) {
        return null;
    }
}
