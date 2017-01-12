package com.kryptnostic.datastore.edm.controllers;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.spark_project.guava.collect.Iterables;
import org.spark_project.guava.collect.Maps;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpServerErrorException;

import com.dataloom.authorization.AclKeyPathFragment;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.authorization.ForbiddenException;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principals;
import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.EntityDataModel;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.internal.Schema;
import com.dataloom.edm.requests.EdmRequest;
import com.dataloom.edm.schemas.manager.HazelcastSchemaManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.kryptnostic.datastore.exceptions.BadRequestException;
import com.kryptnostic.datastore.exceptions.ResourceNotFoundException;
import com.kryptnostic.datastore.services.CassandraEntitySetManager;
import com.kryptnostic.datastore.services.EdmManager;

@RestController
public class EdmController implements EdmApi, AuthorizingComponent {

    @Inject
    private EdmManager                modelService;

    @Inject
    private HazelcastSchemaManager    schemaManager;

    @Inject
    private AuthorizationManager      authorizations;

    @Inject
    private CassandraEntitySetManager entitySetManager;

    @Override
    @RequestMapping(
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public EntityDataModel getEntityDataModel() {
        EntityDataModel edm = modelService.getEntityDataModel();

        Stream<EntitySet> authorizedEntitySets = StreamSupport
                .stream( edm.getEntitySets().spliterator(), false )
                .filter( isAuthorizedObject( Permission.READ ) );

        return new EntityDataModel(
                edm.getNamespaces(),
                edm.getSchemas(),
                edm.getEntityTypes(),
                edm.getPropertyTypes(),
                authorizedEntitySets::iterator );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.datastore.edm.controllers.EdmAPI#getSchemas()
     */
    @Override
    @RequestMapping(
        path = "/" + SCHEMA_BASE_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Schema> getSchemas() {
        return schemaManager.getAllSchemas();
    }

    @Override
    @RequestMapping(
        path = "/" + SCHEMA_BASE_PATH + "/" + NAMESPACE_PATH,
        method = RequestMethod.GET,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Schema> getSchemas( @PathVariable( NAMESPACE ) String namespace ) {
        return schemaManager.getSchemasInNamespace( namespace );

    }

    @Override
    @RequestMapping(
        path = "/" + SCHEMA_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Schema getSchemaContents(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name ) {
        return schemaManager.getSchema( namespace, name );
    }

    @Override
    @RequestMapping(
        path = "/" + SCHEMA_BASE_PATH + "/" + NAMESPACE_PATH )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Schema> getSchemasInNamespace( @PathVariable( NAMESPACE ) String namespace ) {
        return schemaManager.getSchemasInNamespace( namespace );
    }

    @Override
    @RequestMapping(
        path = "/" + SCHEMA_BASE_PATH,
        method = RequestMethod.POST )
    @ResponseStatus( HttpStatus.OK )
    public Void createSchemaIfNotExists( @RequestBody Schema schema ) {
        schemaManager.createOrUpdateSchemas( schema );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + SCHEMA_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH,
        method = RequestMethod.PUT )
    @ResponseStatus( HttpStatus.OK )
    public Void createEmptySchema( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        schemaManager.upsertSchemas( ImmutableSet.of( new FullQualifiedName( namespace, name ) ) );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_SETS_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, UUID> createEntitySets( @RequestBody Set<EntitySet> entitySets ) {
        Map<String, String> badRequests = new HashMap<>();
        Map<String, String> failedRequests = new HashMap<>();
        Map<String, UUID> createdEntitySets = Maps.newHashMapWithExpectedSize( entitySets.size() );
        // TODO: Add access check to make sure user can create entity sets.
        for ( EntitySet entitySet : entitySets ) {
            try {
                modelService.createEntitySet( Principals.getCurrentUser(), entitySet );
                createdEntitySets.put( entitySet.getName(), entitySet.getId() );
            } catch ( IllegalArgumentException e ) {
                badRequests.put( entitySet.getName(), e.getMessage() );
            } catch ( IllegalStateException e ) {
                failedRequests.put( entitySet.getName(), e.getMessage() );
            }
        }

        if ( !badRequests.isEmpty() ) {
            throw new BadRequestException( badRequests.toString() );
        } else if ( !failedRequests.isEmpty() ) {
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, failedRequests.toString() );
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_SETS_BASE_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<EntitySet> getEntitySets() {
        Iterable<AclKeyPathFragment> entitySetAclKeys = authorizations.getAuthorizedObjectsOfType(
                Principals.getCurrentPrincipals(),
                SecurableObjectType.EntitySet,
                EnumSet.of( Permission.READ ) );
        return Iterables.transform( entitySetAclKeys, akpf -> modelService.getEntitySet( akpf.getId() ) );
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_SETS_BASE_PATH + "/" + ID_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public EntitySet getEntitySet( @PathVariable( ID ) UUID entitySetId ) {
        if ( authorizations.checkIfHasPermissions(
                ImmutableList.of( new AclKeyPathFragment( SecurableObjectType.EntitySet, entitySetId ) ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            return modelService.getEntitySet( entitySetId );
        } else {
            throw new ForbiddenException( "Entity set " + entitySetId.toString() + " does not exist." );
        }
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_SETS_BASE_PATH + "/" + ID_PATH,
        method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void deleteEntitySet( @PathVariable( ID ) UUID entitySetName ) {
        try {
            modelService.deleteEntitySet( entitySetName );
        } catch ( IllegalStateException e ) {
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() );
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_TYPE_BASE_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<EntityType> getEntityTypes() {
        return modelService.getEntityTypes();
    }

    @Override
    @RequestMapping(
        path = "/" + SCHEMA_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH,
        method = RequestMethod.PATCH,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void updateSchema(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @RequestBody EdmRequest request ) {
        final Set<UUID> propertyTypes = request.getPropertyTypes();
        final Set<UUID> entityTypes = request.getEntityTypes();
        final FullQualifiedName schemaName = new FullQualifiedName( namespace, name );
        switch ( request.getAction() ) {
            case ADD:
                schemaManager.addEntityTypesToSchema( entityTypes, schemaName );
                schemaManager.removePropertyTypesFromSchema( propertyTypes, schemaName );
                break;
            case REMOVE:
                schemaManager.removeEntityTypesFromSchema( entityTypes, schemaName );
                schemaManager.removePropertyTypesFromSchema( propertyTypes, schemaName );
                break;
            case REPLACE:
                final Set<UUID> existingPropertyTypes = schemaManager.getAllPropertyTypesInSchema( schemaName );
                final Set<UUID> existingEntityTypes = schemaManager.getAllEntityTypesInSchema( schemaName );

                final Set<UUID> propertyTypesToAdd = Sets.difference( propertyTypes, existingPropertyTypes );
                final Set<UUID> propertyTypesToRemove = Sets.difference( existingPropertyTypes, propertyTypes );
                schemaManager.removePropertyTypesFromSchema( propertyTypesToRemove, schemaName );
                schemaManager.addPropertyTypesToSchema( propertyTypesToAdd, schemaName );

                final Set<UUID> entityTypesToAdd = Sets.difference( entityTypes, existingEntityTypes );
                final Set<UUID> entityTypesToRemove = Sets.difference( existingEntityTypes, entityTypes );
                schemaManager.removeEntityTypesFromSchema( entityTypesToAdd, schemaName );
                schemaManager.addEntityTypesToSchema( entityTypesToRemove, schemaName );
                break;
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public UUID createEntityType( @RequestBody EntityType entityType ) {
        modelService.createEntityType( entityType );
        return entityType.getId();
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_TYPE_BASE_PATH + "/" + ID_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public EntityType getEntityType( @PathVariable( ID ) UUID entityTypeId ) {
        try {
            return modelService.getEntityType( entityTypeId );
        } catch ( NullPointerException e ) {
            throw new ResourceNotFoundException( e.getMessage() );
        }
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_TYPE_BASE_PATH + "/" + ID_PATH,
        method = RequestMethod.PUT )
    @ResponseStatus( HttpStatus.OK )
    public Void updatePropertyTypesInEntityType( UUID entityTypeId, Set<UUID> request ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_TYPE_BASE_PATH + "/" + ID_PATH,
        method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void deleteEntityType( @PathVariable( ID ) UUID entityTypeId ) {
        try {
            modelService.deleteEntityType( entityTypeId );
        } catch ( IllegalStateException e ) {
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() );
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PropertyType> getPropertyTypes() {
        return modelService.getPropertyTypes();
    }

    @Override
    @RequestMapping(
        path = "/" + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public UUID createPropertyType( @RequestBody PropertyType propertyType ) {
        try {
            modelService.createPropertyTypeIfNotExists( propertyType );
            return propertyType.getId();
        } catch ( IllegalStateException e ) {
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() );
        }
    }

    @Override
    @RequestMapping(
        path = "/" + PROPERTY_TYPE_BASE_PATH + "/" + ID_PATH,
        method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void deletePropertyType(
            @PathVariable( ID ) UUID propertyTypeId ) {
        try {
            modelService.deletePropertyType( propertyTypeId );
        } catch ( IllegalStateException e ) {
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() );
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + PROPERTY_TYPE_BASE_PATH + "/" + ID_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public PropertyType getPropertyType( @PathVariable( ID ) UUID propertyTypeId ) {
        try {
            return modelService.getPropertyType( propertyTypeId );
        } catch ( NullPointerException e ) {
            throw new ResourceNotFoundException( "Property type not found." );
        }
    }

    @Override
    @RequestMapping(
        path = "/" + NAMESPACE + "/" + NAMESPACE_PATH + "/" + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PropertyType> getPropertyTypesInNamespace( @PathVariable( NAMESPACE ) String namespace ) {
        try {
            return modelService.getPropertyTypesInNamespace( namespace );
        } catch ( NullPointerException e ) {
            throw new ResourceNotFoundException( "Property type not found." );
        }
    }

    @Override
    @RequestMapping(
        path = "/" + IDS + "/" + ENTITY_SETS_BASE_PATH + "/" + NAME_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public UUID getEntitySetId( @PathVariable( NAME ) String entitySetName ) {
        return entitySetManager.getEntitySet( entitySetName ).getId();
    }

    @Override
    @RequestMapping(
        path = "/" + IDS + "/" + PROPERTY_TYPE_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public UUID getPropertyTypeId( String namespace, String name ) {
        return modelService.getTypeAclKey( new FullQualifiedName( namespace, name ) ).getId();
    }

    @Override
    @RequestMapping(
        path = "/" + IDS + "/" + ENTITY_TYPE_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public UUID getEntityTypeId( String namespace, String name ) {
        return modelService.getTypeAclKey( new FullQualifiedName( namespace, name ) ).getId();
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

}
