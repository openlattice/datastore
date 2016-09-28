package com.kryptnostic.datastore.edm.controllers;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import com.kryptnostic.datastore.services.*;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import com.google.common.collect.Maps;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.conductor.rpc.odata.Schema;
import com.kryptnostic.datastore.ServerUtil;
import com.kryptnostic.datastore.services.GetSchemasRequest.TypeDetails;

import retrofit.client.Response;

@RestController
public class EdmController implements EdmApi {
    @Inject
    private EdmManager modelService;

    @Override
    @RequestMapping(
            path = { "", "/" },
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public EntityDataModel getEntityDataModel() {
        return modelService.getEntityDataModel();
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.datastore.edm.controllers.EdmAPI#getSchemas()
     */
    @Override
    @RequestMapping(
            path = SCHEMA_BASE_PATH,
            method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Schema> getSchemas() {
        return ServerUtil.wrapForJackson( modelService.getSchemas() );
    }

    @Override
    @RequestMapping(
            path = SCHEMA_BASE_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Schema> getSchemas( @RequestBody GetSchemasRequest request ) {
        if ( request.getNamespace().isPresent() ) {
            if ( request.getName().isPresent() ) {
                return modelService.getSchema( request.getNamespace().get(),
                        request.getName().get(),
                        request.getLoadDetails() );
            } else {
                return modelService.getSchemasInNamespace( request.getNamespace().get(),
                        request.getLoadDetails() );
            }
        } else {
            return modelService.getSchemas( request.getLoadDetails() );
        }
    }

    @Override
    @RequestMapping(
            path = SCHEMA_BASE_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Schema> getSchemaContents(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name ) {
        return modelService.getSchema( namespace, name, EnumSet.allOf( TypeDetails.class ) );
    }

    @Override
    @RequestMapping(
            path = SCHEMA_BASE_PATH + NAMESPACE_PATH )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Schema> getSchemasInNamespace( @PathVariable( NAMESPACE ) String namespace ) {
        return modelService.getSchemasInNamespace( namespace, EnumSet.allOf( TypeDetails.class ) );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.datastore.edm.controllers.EdmAPI#createSchema(java.lang.String,
     * com.google.common.base.Optional)
     */
    @Override
    @RequestMapping(
            path = SCHEMA_BASE_PATH,
            method = RequestMethod.PUT,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response putSchema( @RequestBody PutSchemaRequest request ) {
        modelService
                .upsertSchema(
                        new Schema().setNamespace( request.getNamespace() ).setName( request.getName() )
                                .setAclId( request.getAclId().or( ACLs.EVERYONE_ACL ) ) );
        return null;
    }

    @Override
    @RequestMapping(
            path = ENTITY_SETS_BASE_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, Boolean> postEntitySets( @RequestBody Set<EntitySet> entitySets ) {
        Map<String, Boolean> results = Maps.newHashMapWithExpectedSize( entitySets.size() );

        for ( EntitySet entitySet : entitySets ) {
            results.put( entitySet.getType().getFullQualifiedNameAsString(),
                    modelService.createEntitySet( entitySet ) );
        }

        return results;
    }

    @Override
    @RequestMapping(
            path = ENTITY_SETS_BASE_PATH,
            method = RequestMethod.PUT,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response putEntitySets( @RequestBody Set<EntitySet> entitySets ) {
        entitySets.forEach( entitySet -> modelService.upsertEntitySet( entitySet ) );
        return null;
    }

    @Override
    @RequestMapping(
            path = ENTITY_SETS_BASE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<EntitySet> getEntitySets() {
        return modelService.getEntitySets();
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.datastore.edm.controllers.EdmAPI#createObjectType(java.lang.String, java.lang.String,
     * java.lang.String, com.kryptnostic.types.ObjectType)
     */
    @Override
    @RequestMapping(
            path = ENTITY_TYPE_BASE_PATH,
            method = RequestMethod.PUT )
    @ResponseStatus( HttpStatus.OK )
    public Response putEntityType( @RequestBody EntityType entityType ) {
        modelService.createEntityType( entityType );
        return null;
    }

    @Override
    @RequestMapping(
            path = ENTITY_TYPE_BASE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<EntityType> getEntityTypes() {
        return modelService.getEntityTypes();
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.datastore.edm.controllers.EdmAPI#createPropertyType(java.lang.String, java.lang.String,
     * java.lang.String, com.kryptnostic.types.ObjectType)
     */
    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_BASE_PATH,
            method = RequestMethod.PUT )
    @ResponseStatus( HttpStatus.OK )
    public Response putPropertyType( @RequestBody PropertyType propertyType ) {
        modelService.upsertPropertyType( propertyType );
        return null;
    }

    @Override
    @RequestMapping(
            path = SCHEMA_BASE_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.PUT,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response addEntityTypeToSchema(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @RequestBody Set<FullQualifiedName> objectTypes ) {
        modelService.addEntityTypesToSchema( namespace, name, objectTypes );
        return null;

    }

    @Override
    @RequestMapping(
            path = SCHEMA_BASE_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.DELETE,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response removeEntityTypeFromSchema(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @RequestBody Set<FullQualifiedName> objectTypes ) {
        modelService.removeEntityTypesFromSchema( namespace, name, objectTypes );
        return null;
    }

    @Override
    @RequestMapping(
            path = ENTITY_TYPE_BASE_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public boolean postEntityType( @RequestBody EntityType objectType ) {
        return modelService.createEntityType( objectType );
    }

    @Override
    @RequestMapping(
            path = ENTITY_TYPE_BASE_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public EntityType getEntityType( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        return modelService.getEntityType( namespace, name );
    }

    @Override
    @RequestMapping(
            path = ENTITY_TYPE_BASE_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Response deleteEntityType( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        modelService.deleteEntityType( new EntityType().setNamespace( namespace ).setName( name ) );
        return null;
    }

    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_BASE_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public boolean postPropertyType( @RequestBody PropertyType propertyType ) {
        return modelService.createPropertyType( propertyType );
    }

    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_BASE_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Response deletePropertyType(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name ) {
        modelService.deletePropertyType( new PropertyType().setNamespace( namespace ).setName( name ) );
        return null;
    }

    @Override
    @RequestMapping(
            path = PROPERTY_TYPE_BASE_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public PropertyType getPropertyType( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        return modelService.getPropertyType( new FullQualifiedName( namespace, name ) );
    }
}
