package com.kryptnostic.datastore.edm.controllers;

import java.util.UUID;

import javax.inject.Inject;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.google.common.base.Optional;
import com.kryptnostic.datastore.util.UUIDs.ACLs;
import com.kryptnostic.types.Container;
import com.kryptnostic.types.Namespace;
import com.kryptnostic.types.ObjectType;
import com.kryptnostic.types.PropertyType;
import com.kryptnostic.types.Schema;
import com.kryptnostic.types.services.EdmManager;

@Controller
public class EdmController implements EdmApi {
    @Inject
    private EdmManager modelService;

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.datastore.edm.controllers.EdmAPI#getSchemas()
     */
    @Override
    @ResponseStatus( HttpStatus.OK )
    @RequestMapping(
        path = { "", "/" },
        method = RequestMethod.GET )
    public Iterable<Namespace> getSchemas() {
        return modelService.getNamespaces();
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.datastore.edm.controllers.EdmAPI#createSchema(java.lang.String,
     * com.google.common.base.Optional)
     */
    @Override
    @RequestMapping(
        path = "/{namespace}",
        method = RequestMethod.PUT )
    public void putSchema(
            @PathVariable( "namespace" ) String namespace,
            @RequestBody Optional<UUID> aclId ) {
        modelService.createNamespace( namespace, aclId.or( ACLs.EVERYONE_ACL ) );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.datastore.edm.controllers.EdmAPI#createContainer(java.lang.String, java.lang.String,
     * com.google.common.base.Optional)
     */
    @Override
    @RequestMapping(
        path = "/{namespace}/containers",
        method = RequestMethod.PUT )
    public void putContainer(
            @PathVariable( "namespace" ) String namespace,
            @RequestBody Container container ) {
        modelService.upsertContainer( container );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.datastore.edm.controllers.EdmAPI#createObjectType(java.lang.String, java.lang.String,
     * java.lang.String, com.kryptnostic.types.ObjectType)
     */
    @Override
    @RequestMapping(
        path = "/{namespace}/types/objects",
        method = RequestMethod.PUT )
    public void putObjectType(
            @PathVariable( "namespace" ) String namespace,
            @PathVariable( "objectType" ) String objectType,
            @RequestBody ObjectType typeInfo ) {
        modelService.createObjectType( typeInfo );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.datastore.edm.controllers.EdmAPI#createPropertyType(java.lang.String, java.lang.String,
     * java.lang.String, com.kryptnostic.types.ObjectType)
     */
    @Override
    @RequestMapping(
        path = "/{namespace}/types/properties",
        method = RequestMethod.PUT )
    public void putPropertyType(
            @PathVariable( "namespace" ) String namespace,
            @RequestBody PropertyType propertyType ) {
        modelService.upsertPropertyType( propertyType );
    }

    @Override
    public Schema getSchema( String namespace ) {
        return modelService.getSchema( namespace );
    }

    @Override
    public void addObjectTypeToContainer( String namespace, String container, String objectType ) {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeObjectTypeFromContainer( String namespace, String container, String objectType ) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean postContainer( String namespace, Container container ) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean postObjectType( String namespace, ObjectType objectType ) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void putObjectType( String namespace, ObjectType typeInfo ) {
        // TODO Auto-generated method stub

    }

    @Override
    public void deleteObjectType( String namespace, String objectType ) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean postPropertyType( String namespace, PropertyType propertyType ) {
        modelService.createPropertyType( namespace, propertyType.getType(), propertyType.getTypename() , propertyType.getDatatype(), propertyType.getMultiplicity() );
        return false;
    }

    @Override
    public void deletePropertyType( String namespace, String propertyType ) {
        modelService.deletePropertyType( new PropertyType().setNamespace( namespace ).setType( propertyType ) );

    }

}
