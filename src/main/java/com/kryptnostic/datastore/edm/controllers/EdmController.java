package com.kryptnostic.datastore.edm.controllers;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.kryptnostic.datastore.util.UUIDs.ACLs;
import com.kryptnostic.datastore.util.Util;
import com.kryptnostic.types.EntityDataModel;
import com.kryptnostic.types.EntitySet;
import com.kryptnostic.types.EntityType;
import com.kryptnostic.types.GetSchemasRequest;
import com.kryptnostic.types.GetSchemasRequest.TypeDetails;
import com.kryptnostic.types.PropertyType;
import com.kryptnostic.types.Schema;
import com.kryptnostic.types.services.EdmManager;

import retrofit.client.Response;

@Controller
public class EdmController implements EdmApi {
    @Inject
    private EdmManager modelService;

    @Override
    public EntityDataModel getEntityDataModel() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.datastore.edm.controllers.EdmAPI#getSchemas()
     */
    @Override
    @RequestMapping(
        path = SCHEMA_BASE_PATH,
        method = RequestMethod.GET )
    @ResponseBody
    public Iterable<Schema> getSchemas() {
        return Util.wrapForJackson( modelService.getSchemas() );
    }

    @Override
    @RequestMapping(
        path = SCHEMA_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseBody
    public Iterable<Schema> getSchemas( @RequestBody GetSchemasRequest request ) {
        Iterable<Schema> schemas;

        if ( request.getNamespace().isPresent() ) {
            if ( request.getName().isPresent() ) {
                schemas = ImmutableList
                        .of( modelService.getSchema( request.getNamespace().get(), request.getName().get() ) );
            } else {
                schemas = modelService.getSchemasInNamespace( request.getNamespace().get() );
            }
        } else {
            schemas = modelService.getSchemas();
        }

        // Enrich schemas with requested details
        schemas.forEach( schema -> {
            if ( request.getLoadDetails().contains( TypeDetails.ENTITY_TYPES ) ) {
                modelService.enrichSchemaWithEntityTypes( schema );
            }

            if ( request.getLoadDetails().contains( TypeDetails.PROPERTY_TYPES ) ) {
                modelService.enrichSchemaWithEntityTypes( schema );
            }
        } );

        return schemas;
    }

    @Override
    public Schema getSchemaContents( String namespace, String name ) {
        return modelService.getSchema( namespace, name );
    }

    @Override
    @RequestMapping(
        path = SCHEMA_BASE_PATH + NAMESPACE_PATH )
    @ResponseBody
    public Iterable<Schema> getSchemasInNamespace( String namespace ) {
        return modelService.getSchemasInNamespace( namespace );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.datastore.edm.controllers.EdmAPI#createSchema(java.lang.String,
     * com.google.common.base.Optional)
     */
    @Override
    @RequestMapping(
        path = "/namespace",
        method = RequestMethod.PUT )
    @ResponseStatus( HttpStatus.OK )
    public Response putSchema(
            @PathVariable( "namespace" ) String namespace,
            @RequestBody Optional<UUID> aclId ) {
        modelService
                .upsertSchema(
                        new Schema().setNamespace( namespace ).setAclId( aclId.or( ACLs.EVERYONE_ACL ) ) );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/sets/entity" )
    public @ResponseBody Map<String, Boolean> postEntitySets( Set<EntitySet> entitySets ) {
        Map<String, Boolean> results = Maps.newHashMapWithExpectedSize( entitySets.size() );

        for ( EntitySet entitySet : entitySets ) {
            results.put( entitySet.getType().getFullQualifiedNameAsString(),
                    modelService.createEntitySet( entitySet ) );
        }

        return results;
    }

    @Override
    public Response putEntitySets( Set<EntitySet> entitySets ) {
        entitySets.forEach( entitySet -> modelService.upsertEntitySet( entitySet ) );
        return null;
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.datastore.edm.controllers.EdmAPI#createObjectType(java.lang.String, java.lang.String,
     * java.lang.String, com.kryptnostic.types.ObjectType)
     */
    @Override
    @RequestMapping(
        path = "/{namespace}/types/entity",
        method = RequestMethod.PUT )
    @ResponseStatus( HttpStatus.OK )
    public Response putEntityType(
            @RequestBody EntityType entityType ) {
        modelService.createEntityType( entityType );
        return null;
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
    @ResponseBody
    public boolean postEntityType( @RequestBody EntityType objectType ) {
        return modelService.createEntityType( objectType );
    }

    @Override
    @RequestMapping(
        path = ENTITY_TYPE_BASE_PATH + NAMESPACE_PATH + NAME_PATH,
        method = RequestMethod.GET )
    @ResponseBody
    public EntityType getEntityType( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        return modelService.getEntityType( namespace, name );
    }

    @Override
    @RequestMapping(
        path = ENTITY_TYPE_BASE_PATH + NAMESPACE_PATH + NAME_PATH,
        method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Response deleteEntityType( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        modelService.deleteEntityType( new EntityType().setNamespace( namespace ).setType( name ) );
        return null;
    }

    @Override
    @RequestMapping(
        path = PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseBody
    public boolean postPropertyType( PropertyType propertyType ) {
        return modelService.createPropertyType(
                propertyType.getNamespace(),
                propertyType.getType(),
                propertyType.getTypename(),
                propertyType.getDatatype(),
                propertyType.getMultiplicity() );
    }

    @Override
    @RequestMapping(
        path = PROPERTY_TYPE_BASE_PATH + NAMESPACE_PATH + NAME_PATH )
    public Response deletePropertyType(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String propertyType ) {
        modelService.deletePropertyType( new PropertyType().setNamespace( namespace ).setType( propertyType ) );
        return null;
    }

}
