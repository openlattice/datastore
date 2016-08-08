package com.kryptnostic.types.controllers;

import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;
import javax.ws.rs.core.MediaType;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.types.services.TypesService;
import com.kryptnostic.v2.storage.api.TypesApi;
import com.kryptnostic.v2.storage.models.Scope;

@Controller
@RequestMapping( TypesApi.CONTROLLER )
public class DataStoreController implements TypesApi {

    
    @Inject
    private TypesService typesService;

    @Override
    @RequestMapping(
        value = CONTROLLER,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON )
    @ResponseStatus( HttpStatus.OK )
    public @ResponseBody Map<String, Scope> getScopes() {
        return typesService.getScopes();
    }

    @Override
    @RequestMapping(
        value = CONTROLLER + SCOPE_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON )
    @ResponseStatus( HttpStatus.OK )
    public @ResponseBody Scope getScopeInformation( @PathVariable( SCOPE ) String scope) {
        return typesService.getScopeInformation( scope );
    }

    @Override
    @RequestMapping(
        value = CONTROLLER + SCOPE_PATH + TYPE_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON )
    @ResponseStatus( HttpStatus.OK )
    public @ResponseBody UUID getOrCreateUUIDForType(
            @PathVariable( SCOPE ) String scope,
            @PathVariable( TYPE ) String type) {
        return typesService.getOrCreateUuidForType( scope, type );
    }
}
