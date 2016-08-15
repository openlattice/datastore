package com.kryptnostic.datastore.edm.controllers;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.kryptnostic.datastore.util.UUIDs.ACLs;
import com.kryptnostic.types.EntitySet;
import com.kryptnostic.types.EntityType;
import com.kryptnostic.types.PropertyType;
import com.kryptnostic.types.Schema;
import com.kryptnostic.types.SchemaMetadata;
import com.kryptnostic.types.services.EdmManager;

import retrofit.client.Response;

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
    public Iterable<SchemaMetadata> getSchemas() {
        return modelService.getSchemaMetadata();
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
    @ResponseStatus( HttpStatus.OK )
    public Response putSchema(
            @PathVariable( "namespace" ) String namespace,
            @RequestBody Optional<UUID> aclId ) {
        modelService
                .upsertSchema(
                        new SchemaMetadata().setNamespace( namespace ).setAclId( aclId.or( ACLs.EVERYONE_ACL ) ) );
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
    public void putPropertyType(
            @PathVariable( "namespace" ) String namespace,
            @RequestBody PropertyType propertyType ) {
        modelService.upsertPropertyType( propertyType );
    }

    @Override
    @RequestMapping(
        path = "/schema/{namespace}/{name}",
        method = RequestMethod.PUT )
    public Schema getSchema( String namespace, String name ) {
        return modelService.getSchema( namespace, name );
    }

    @Override
    public Response addEntityTypeToSchema( String namespace, String container, @RequestBody Set<String> objectTypes ) {
        modelService.addEntityTypesToSchema( namespace, container, objectTypes );
        return null;

    }

    @Override
    public void removeEntityTypeFromSchema(
            String namespace,
            String container,
            @RequestBody Set<String> objectTypes ) {
        modelService.removeEntityTypesFromSchema( namespace, container, objectTypes );

    }

    @Override
    public boolean postEntityType( String namespace, EntityType objectType ) {
        return modelService.createEntityType( objectType );
    }

    @Override
    public void deleteEntityType( String namespace, String objectType ) {
        modelService.deleteEntityType( new EntityType().setNamespace( namespace ).setType( objectType ) );
    }

    @Override
    public boolean postPropertyType( String namespace, PropertyType propertyType ) {
        modelService.createPropertyType( namespace,
                propertyType.getType(),
                propertyType.getTypename(),
                propertyType.getDatatype(),
                propertyType.getMultiplicity() );
        return false;
    }

    @Override
    public void deletePropertyType( String namespace, String propertyType ) {
        modelService.deletePropertyType( new PropertyType().setNamespace( namespace ).setType( propertyType ) );
    }

    @Override
    public Iterable<Schema> getSchemasInNamespace( String namespace ) {
        // TODO Auto-generated method stub
        return null;
    }

}
