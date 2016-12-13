package com.kryptnostic.datastore.edm.controllers;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Inject;
import javax.validation.ConstraintViolation;
import javax.validation.Valid;
import javax.validation.groups.Default;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.dataloom.authorization.requests.Permission;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.EntityDataModel;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntitySetWithPermissions;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.EntityTypeWithDetails;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.internal.Schema;
import com.dataloom.edm.requests.GetSchemasRequest;
import com.dataloom.edm.requests.GetSchemasRequest.TypeDetails;
import com.dataloom.edm.validation.ValidateFullQualifiedName;
import com.dataloom.edm.validation.tags.Extended;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.kryptnostic.datastore.ServerUtil;
import com.kryptnostic.datastore.exceptions.BatchExceptions;
import com.kryptnostic.datastore.exceptions.ForbiddenException;
import com.kryptnostic.datastore.exceptions.ResourceNotFoundException;
import com.kryptnostic.datastore.services.ActionAuthorizationService;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.PermissionsService;
import com.kryptnostic.datastore.util.ErrorsDTO;

@RestController
public class EdmController implements EdmApi {
    @Inject
    private EdmManager                 modelService;

    @Inject
    private PermissionsService         ps;

    @Inject
    private ActionAuthorizationService authzService;

    @Inject
    private LocalValidatorFactoryBean  validator;

    @Override
    @RequestMapping(
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
        path = "/" + SCHEMA_BASE_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Schema> getSchemas() {
        return ServerUtil.wrapForJackson( modelService.getSchemas() );
    }

    @Override
    @RequestMapping(
        path = "/" + SCHEMA_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Schema> getSchemas( @RequestBody @Valid GetSchemasRequest request ) {
        Iterable<Schema> results;

        if ( request.getNamespace().isPresent() && StringUtils.isNotBlank( request.getNamespace().get() ) ) {
            if ( request.getName().isPresent() && StringUtils.isNotBlank( request.getName().get() ) ) {
                results = Arrays.asList( modelService.getSchema( request.getNamespace().get(),
                        request.getName().get(),
                        request.getLoadDetails() ) );
            } else {
                results = modelService.getSchemasInNamespace( request.getNamespace().get(),
                        request.getLoadDetails() );
            }
        } else {
            results = modelService.getSchemas( request.getLoadDetails() );
        }

        if ( results != null ) {
            return results;
        } else {
            throw new ResourceNotFoundException();
        }
    }

    @Override
    @RequestMapping(
        path = "/" + SCHEMA_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Schema getSchemaContents(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name ) {
        Schema result = modelService.getSchema( namespace, name, EnumSet.allOf( TypeDetails.class ) );
        if( result != null ){
            return result;
        } else {
            throw new ResourceNotFoundException();
        }
    }

    @Override
    @RequestMapping(
        path = "/" + SCHEMA_BASE_PATH + "/" + NAMESPACE_PATH )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Schema> getSchemasInNamespace( @PathVariable( NAMESPACE ) String namespace ) {
        return modelService.getSchemasInNamespace( namespace, EnumSet.allOf( TypeDetails.class ) );
    }

    @Override
    @RequestMapping(
        path = "/" + SCHEMA_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH,
        method = RequestMethod.PUT )
    @ResponseStatus( HttpStatus.OK )
    public Void createEmptySchema( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        modelService
                .upsertSchema(
                        new Schema().setNamespace( namespace ).setName( name ) );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_SETS_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void postEntitySets( @RequestBody Set<EntitySet> entitySets ) {
        ErrorsDTO dto = new ErrorsDTO();

        // TODO Cascade validation not working. Temporarily workaround as follows.
        for ( EntitySet entitySet : entitySets ) {
            Set<ConstraintViolation<EntitySet>> validationErrors = validator.validate( entitySet, Default.class, Extended.class );
            if ( !validationErrors.isEmpty() ) {
                for ( ConstraintViolation<EntitySet> error : validationErrors ) {
                    dto.addError( IllegalArgumentException.class.getName(), entitySet.getName(), error.getMessage() );
                }
            }
        }

        if ( !dto.isEmpty() ) {
            throw new BatchExceptions( dto, HttpStatus.BAD_REQUEST );
        }

        for ( EntitySet entitySet : entitySets ) {
            try {
                modelService.createEntitySet( Optional.fromNullable( authzService.getUserId() ), entitySet );
            } catch ( Exception e ) {
                dto.addError( e.getClass().getName(), entitySet.getName(), e.getMessage() );
            }
        }

        if ( !dto.isEmpty() ) {
            throw new BatchExceptions( dto, HttpStatus.INTERNAL_SERVER_ERROR );
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_SETS_BASE_PATH,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void putEntitySets( @Valid @RequestBody Set<EntitySet> entitySets ) {
        ErrorsDTO dto = new ErrorsDTO();

        // TODO Cascade validation not working. Temporarily workaround as follows.
        for ( EntitySet entitySet : entitySets ) {
            Set<ConstraintViolation<EntitySet>> validationErrors = validator.validate( entitySet, Default.class, Extended.class );
            if ( !validationErrors.isEmpty() ) {
                for ( ConstraintViolation<EntitySet> error : validationErrors ) {
                    dto.addError( IllegalArgumentException.class.getName(), entitySet.getName(), error.getMessage() );
                }
            }
        }

        if ( !dto.isEmpty() ) {
            throw new BatchExceptions( dto, HttpStatus.BAD_REQUEST );
        }

        for ( EntitySet entitySet : entitySets ) {
            try {
                modelService.upsertEntitySet( entitySet );
            } catch ( Exception e ) {
                dto.addError( e.getClass().getName(), entitySet.getName(), e.getMessage() );
            }
        }

        if ( !dto.isEmpty() ) {
            throw new BatchExceptions( dto, HttpStatus.INTERNAL_SERVER_ERROR );
        }

        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_SETS_BASE_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<EntitySetWithPermissions> getEntitySets(
            @RequestParam(
                value = IS_OWNER,
                required = false ) Boolean isOwner ) {
        String userId = authzService.getUserId();
        List<String> currentRoles = authzService.getRoles();

        Set<String> ownedSets = Sets.newHashSet( modelService.getEntitySetNamesUserOwns( userId ) );

        if ( isOwner != null ) {
            if ( isOwner ) {
                // isOwner = true -> return all entity sets owned
                EnumSet<Permission> allPermissions = EnumSet.allOf( Permission.class );
                return StreamSupport.stream( modelService.getEntitySetsUserOwns( userId ).spliterator(), false )
                        .map( entitySet -> new EntitySetWithPermissions().setEntitySet( entitySet )
                                .setPermissions( allPermissions )
                                .setIsOwner( true ) )
                        .collect( Collectors.toList() );
            } else {
                // isOwner = false -> return all entity sets not owned, with permissions
                return StreamSupport.stream( modelService.getEntitySets().spliterator(), false )
                        .filter( entitySet -> !ownedSets.contains( entitySet.getName() ) )
                        .filter( entitySet -> authzService.getEntitySet( entitySet.getName() ) )
                        .map( entitySet -> new EntitySetWithPermissions().setEntitySet( entitySet )
                                .setPermissions( ps
                                        .getEntitySetAclsForUser( userId, currentRoles, entitySet.getName() ) )
                                .setIsOwner( false ) )
                        .collect( Collectors.toList() );
            }
        } else {
            // No query parameter -> return all entity sets
            return StreamSupport.stream( modelService.getEntitySets().spliterator(), false )
                    .filter( entitySet -> authzService.getEntitySet( entitySet.getName() ) )
                    .map( entitySet -> new EntitySetWithPermissions().setEntitySet( entitySet )
                            .setPermissions( ps.getEntitySetAclsForUser( userId, currentRoles, entitySet.getName() ) )
                            .setIsOwner( ownedSets.contains( entitySet.getName() ) ) )
                    .collect( Collectors.toList() );
        }
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_SETS_BASE_PATH + "/" + NAME_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public EntitySet getEntitySet( @PathVariable( NAME ) String entitySetName ) {
        if ( modelService.checkEntitySetExists( entitySetName ) && authzService.getEntitySet( entitySetName ) ) {
            return modelService.getEntitySet( entitySetName );
        } else {
            throw new ForbiddenException();
        }
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_SETS_BASE_PATH + "/" + NAME_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void assignEntityToEntitySet(
            @PathVariable( NAME ) String entitySetName,
            @RequestBody Set<UUID> entityIds ) {
        if ( modelService.checkEntitySetExists( entitySetName ) &&
                authzService.assignEntityToEntitySet( entitySetName ) ) {
            ErrorsDTO dto = new ErrorsDTO();

            for ( UUID entityId : entityIds ) {
                try {
                    modelService.assignEntityToEntitySet( entityId, entitySetName );
                } catch ( Exception e ) {
                    dto.addError( e.getClass().getName(), entityId.toString(), e.getMessage() );
                }
            }

            if ( !dto.isEmpty() ) {
                throw new BatchExceptions( dto, HttpStatus.INTERNAL_SERVER_ERROR );
            }
        } else {
            throw new ForbiddenException();            
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_SETS_BASE_PATH + "/" + NAME_PATH,
        method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void deleteEntitySet( String entitySetName ) {
        modelService.deleteEntitySet( entitySetName );
        return null;
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.datastore.edm.controllers.EdmAPI#createObjectType(java.lang.String, java.lang.String,
     * java.lang.String, com.kryptnostic.types.ObjectType)
     */
    @Override
    @RequestMapping(
        path = "/" + ENTITY_TYPE_BASE_PATH,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void putEntityType( @RequestBody @Valid EntityType entityType ) {
        modelService.upsertEntityType( Optional.fromNullable( authzService.getUserId() ), entityType );
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
        path = "/" + ENTITY_TYPE_BASE_PATH + "/" + DETAILS_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<EntityTypeWithDetails> getEntityTypesWithDetails() {
        return StreamSupport.stream( modelService.getEntityTypes().spliterator(), false )
                .map( entityType -> new EntityTypeWithDetails(
                        entityType,
                        entityType.getProperties().stream()
                                .collect(
                                        Collectors.toMap( fqn -> fqn, fqn -> modelService.getPropertyType( fqn ) ) ) ) )
                .collect( Collectors.toSet() );
    }

    @Override
    @RequestMapping(
        path = "/" + SCHEMA_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH + "/" + ADD_ENTITY_TYPES_PATH,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void addEntityTypesToSchema(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @Valid @RequestBody Set<@ValidateFullQualifiedName FullQualifiedName> entityTypes ) {
        // TODO Cascade validation not working
        modelService.addEntityTypesToSchema( namespace, name, entityTypes );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + SCHEMA_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH + "/" + DELETE_ENTITY_TYPES_PATH,
        method = RequestMethod.DELETE,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void removeEntityTypeFromSchema(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @Valid @RequestBody Set<@ValidateFullQualifiedName FullQualifiedName> objectTypes ) {
        // TODO Cascade validation not working
        modelService.removeEntityTypesFromSchema( namespace, name, objectTypes );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void postEntityType( @RequestBody @Valid EntityType objectType ) {
        modelService.createEntityType( Optional.fromNullable( authzService.getUserId() ), objectType );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_TYPE_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public EntityType getEntityType( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        return modelService.getEntityType( namespace, name );
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_TYPE_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH,
        method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void deleteEntityType( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        FullQualifiedName entityTypeFqn = new FullQualifiedName( namespace, name );
        modelService.deleteEntityType( entityTypeFqn );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void createPropertyType( @RequestBody @Valid PropertyType propertyType ) {
        modelService.createPropertyType( propertyType );
        return null;
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.datastore.edm.controllers.EdmAPI#createPropertyType(java.lang.String, java.lang.String,
     * java.lang.String, com.kryptnostic.types.ObjectType)
     */
    @Override
    @RequestMapping(
        path = "/" + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.PUT )
    @ResponseStatus( HttpStatus.OK )
    public Void putPropertyType( @RequestBody @Valid PropertyType propertyType ) {
        modelService.upsertPropertyType( propertyType );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + PROPERTY_TYPE_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH,
        method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void deletePropertyType(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name ) {
        FullQualifiedName propertyTypeFqn = new FullQualifiedName( namespace, name );
        modelService.deletePropertyType( propertyTypeFqn );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + PROPERTY_TYPE_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public PropertyType getPropertyType(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name ) {
        return modelService.getPropertyType( new FullQualifiedName( namespace, name ) );
    }

    @Override
    @RequestMapping(
        path = "/" + PROPERTY_TYPE_BASE_PATH + "/" + NAMESPACE_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PropertyType> getPropertyTypesInNamespace( @PathVariable( NAMESPACE ) String namespace ) {
        return modelService.getPropertyTypesInNamespace( namespace );
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_TYPE_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH + "/" + ADD_PROPERTY_TYPES_PATH,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void addPropertyTypesToEntityType(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @Valid @RequestBody Set<@ValidateFullQualifiedName FullQualifiedName> properties ) {
        // TODO Cascade validation not working
        modelService.addPropertyTypesToEntityType( namespace, name, properties );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_TYPE_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH + "/" + DELETE_PROPERTY_TYPES_PATH,
        method = RequestMethod.DELETE,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void removePropertyTypesFromEntityType(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @Valid @RequestBody Set<@ValidateFullQualifiedName FullQualifiedName> properties ) {
        // TODO Cascade validation not working
        modelService.removePropertyTypesFromEntityType( namespace, name, properties );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + SCHEMA_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH + "/" + ADD_PROPERTY_TYPES_PATH,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void addPropertyTypesToSchema(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @Valid @RequestBody Set<@ValidateFullQualifiedName FullQualifiedName> properties ) {
        modelService.addPropertyTypesToSchema( namespace, name, properties );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + SCHEMA_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH + "/" + DELETE_PROPERTY_TYPES_PATH,
        method = RequestMethod.DELETE,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void removePropertyTypesFromSchema(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @Valid @RequestBody Set<@ValidateFullQualifiedName FullQualifiedName> properties ) {
        modelService.removePropertyTypesFromSchema( namespace, name, properties );
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

}
