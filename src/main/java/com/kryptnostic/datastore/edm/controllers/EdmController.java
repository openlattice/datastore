package com.kryptnostic.datastore.edm.controllers;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.kryptnostic.types.services.DataModelService;

@Controller
public class EdmController {
    private DataModelService modelService;

    @RequestMapping(
        path = "/{namespace}/{container}",
        method = RequestMethod.PUT )
    public void createSchema(
            @PathVariable( "namespace" ) String namespace,
            @PathVariable( "container" ) String container ) {
        //modelService.createContainer

    }

    @ResponseStatus( HttpStatus.OK )
    @RequestMapping(
        path = "/namespace",
        method = RequestMethod.GET )
    public void getSchemas() {

    }

}
