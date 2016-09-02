package com.kryptnostic.datastore.data.controllers;

import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.kryptnostic.datastore.odata.EntityDataModel;
import com.kryptnostic.datastore.services.DataStorageClient;
import com.kryptnostic.datastore.services.EdmManager;

@Controller
@RequestMapping( "/raw" )
public class StorageController {
    @Inject
    private EdmManager          modelService;

    @Inject
    private DataStorageClient storage;

    @RequestMapping(
        path = { "", "/" },
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseBody
    public EntityDataModel getEntityDataModel() {
        return modelService.getEntityDataModel();
    }

    @RequestMapping(
        path = { "/object/{id}" },
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseBody
    public Map<String, Object> getObject( @PathVariable("id") UUID objectId) {
        return storage.getObject( objectId );
    }

}
