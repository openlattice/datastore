package com.kryptnostic.datastore.data.controllers;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.datastore.services.DataStorageClient;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.EntityDataModel;
import com.kryptnostic.datastore.services.EntityStorageClient;

@Controller
@RequestMapping( "/raw" )
public class StorageController {
    @Inject
    private EdmManager          modelService;

    @Inject
    private DataStorageClient storage;

    final String QUERY_BASE_PATH 		 = "/query";
    final String ENTITY_TYPE_BASE_PATH   = "/entity/type";

    @RequestMapping(
        path = { "", "/" },
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseBody
    public EntityDataModel getEntityDataModel() {
        return modelService.getEntityDataModel();
    }
    
    /**
     * Retrieves entity set by FullQualifiedName, passed as JSON
     * @param fqn
     * @return
     */
    @RequestMapping(
        path = QUERY_BASE_PATH + ENTITY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Iterable< SetMultimap<FullQualifiedName, Object> > readAllEntitiesOfType( @RequestBody FullQualifiedName fqn ) {
    	return storage.readAllEntitiesOfType( fqn );
    }  
    
    /**
     * Retrieves entity set by FullQualifiedName, passed as path variable
     * @param fqn
     * @return
     */
    @RequestMapping(
        path = QUERY_BASE_PATH + ENTITY_TYPE_BASE_PATH + "{fqn}",
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Iterable< SetMultimap<FullQualifiedName, Object> > readAllEntitiesOfType( @PathVariable String fqnAsString ) {
    	return storage.readAllEntitiesOfType( new FullQualifiedName(fqnAsString) );
    }  
    
    /**
     * Retrieves entity set by FullQualifiedName, passed as RequestParam
     * @param namespace
     * @param name
     * @return
     */
    @RequestMapping(
        path = QUERY_BASE_PATH + ENTITY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Iterable< SetMultimap<FullQualifiedName, Object> > readAllEntitiesOfType( @RequestParam("namespace") String namespace, @RequestParam("name") String name ) {
    	return storage.readAllEntitiesOfType( new FullQualifiedName(namespace, name) );
    }  

}
