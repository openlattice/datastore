package com.kryptnostic.datastore.edm.controllers;

import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.datastore.util.UUIDs.ACLs;
import com.kryptnostic.types.Namespace;
import com.kryptnostic.types.ObjectType;
import com.kryptnostic.types.services.EdmManager;

@Controller
public class EdmController {
    private EdmManager modelService;

    @ResponseStatus( HttpStatus.OK )
    @RequestMapping(
        path = { "", "/" },
        method = RequestMethod.GET )
    public Iterable<Namespace> getSchemas() {
        return modelService.getNamespaces( ImmutableSet.of( ACLs.EVERYONE_ACL ) );
    }

    @RequestMapping(
        path = "/{namespace}",
        method = RequestMethod.PUT )
    public void createSchema(
            @PathVariable( "namespace" ) String namespace,
            @RequestBody Optional<UUID> aclId ) {
        modelService.createNamespace( namespace, aclId.or( ACLs.EVERYONE_ACL ) );
    }

    @RequestMapping(
        path = "/{namespace}/{container}",
        method = RequestMethod.PUT )
    public void createContainer(
            @PathVariable( "namespace" ) String namespace,
            @PathVariable( "container" ) String container,
            @RequestBody Optional<UUID> aclId ) {
        modelService.createContainer( namespace, container, aclId.or( ACLs.EVERYONE_ACL ) );
    }

    @RequestMapping(
        path = "/{namespace}/{container}/{objectType}",
        method = RequestMethod.PUT )
    public void createObjectType(
            @PathVariable( "namespace" ) String namespace,
            @PathVariable( "container" ) String container,
            @PathVariable( "objectType" ) String objectType,
            @RequestBody ObjectType typeInfo ) {
        modelService.createObjectType( typeInfo );
    }
    
    @RequestMapping(
            path = "/{namespace}/{container}/{objectType}",
            method = RequestMethod.PUT )
        public void createPropertyType(
                @PathVariable( "namespace" ) String namespace,
                @PathVariable( "container" ) String container,
                @PathVariable( "objectType" ) String objectType,
                @RequestBody ObjectType typeInfo ) {
            modelService.createObjectType( typeInfo );
        }

}
