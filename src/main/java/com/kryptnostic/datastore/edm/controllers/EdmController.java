package com.kryptnostic.datastore.edm.controllers;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpServerErrorException;

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
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.kryptnostic.datastore.ServerUtil;
import com.kryptnostic.datastore.exceptions.BadRequestException;
import com.kryptnostic.datastore.exceptions.ResourceNotFoundException;
import com.kryptnostic.datastore.services.ActionAuthorizationService;
import com.kryptnostic.datastore.services.EdmManager;

import com.kryptnostic.datastore.services.PermissionsService;

@RestController
public class EdmController implements EdmApi {
    @Inject
    private EdmManager modelService;

    @Inject
    private PermissionsService ps;

    @Inject
    private ActionAuthorizationService authzService;

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
    public Iterable<Schema> getSchemas( @RequestBody GetSchemasRequest request ) {
        try{
            if ( request.getNamespace().isPresent() ) {
                if ( request.getName().isPresent() ) {
                    return Arrays.asList( modelService.getSchema( request.getNamespace().get(),
                            request.getName().get(),
                            request.getLoadDetails() ) );
                } else {
                    return modelService.getSchemasInNamespace( request.getNamespace().get(),
                            request.getLoadDetails() );
                }
            } else {
                return modelService.getSchemas( request.getLoadDetails() );
            }
        } catch ( IllegalArgumentException e ){
            throw new BadRequestException( e.getMessage() );
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
        try { 
            return modelService.getSchema( namespace, name, EnumSet.allOf( TypeDetails.class ) );
        } catch ( IllegalArgumentException e ){
            throw new BadRequestException( e.getMessage() );
        }
    }

    @Override
    @RequestMapping(
        path = "/" + SCHEMA_BASE_PATH + "/" + NAMESPACE_PATH )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Schema> getSchemasInNamespace( @PathVariable( NAMESPACE ) String namespace ) {
        try{
            return modelService.getSchemasInNamespace( namespace, EnumSet.allOf( TypeDetails.class ) );
        } catch ( IllegalArgumentException e ){
            throw new BadRequestException( e.getMessage() );
        }        
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
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void postEntitySets( @RequestBody Set<EntitySet> entitySets ) {        
        Map<String, String> badRequests = new HashMap<>();
        Map<String, String> failedRequests = new HashMap<>();
        
        for ( EntitySet entitySet : entitySets ) {
              try{
                  modelService.createEntitySet( Optional.fromNullable( authzService.getUserId() ), entitySet );
              } catch ( IllegalArgumentException e){
                  badRequests.put( entitySet.getName(), e.getMessage() );
              } catch ( IllegalStateException e){
                  failedRequests.put( entitySet.getName(), e.getMessage() );
              }
        }
        
        if( !badRequests.isEmpty() ){
            throw new BadRequestException( badRequests.toString() );
        } else if ( !failedRequests.isEmpty() ){
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, failedRequests.toString() ); 
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_SETS_BASE_PATH,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void putEntitySets( @RequestBody Set<EntitySet> entitySets ) {

        entitySets.forEach( entitySet -> {
            if ( modelService.checkEntitySetExists( entitySet.getName() ) ) {
                if ( authzService.alterEntitySet( entitySet.getName() ) ) {
                        modelService.upsertEntitySet( entitySet );
                }
            } else {
                    modelService.createEntitySet( Optional.of( authzService.getUserId() ), entitySet );
            }
        } );

        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_SETS_BASE_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<EntitySetWithPermissions> getEntitySets(
            @RequestParam( value = IS_OWNER, required = false ) Boolean isOwner ) {
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
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_SETS_BASE_PATH + "/" + NAME_PATH,
        method = RequestMethod.POST )
    @ResponseStatus( HttpStatus.OK )
    public Void assignEntityToEntitySet( String entitySetName, Set<UUID> entityIds ) {
        if ( modelService.checkEntitySetExists( entitySetName ) && 
                authzService.assignEntityToEntitySet( entitySetName ) ) {
            try{
                for ( UUID entityId : entityIds ) {
                    modelService.assignEntityToEntitySet( entityId, entitySetName );
                }
            } catch( IllegalStateException e){
                throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() ); 
            }
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_SETS_BASE_PATH + "/" + NAME_PATH,
        method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void deleteEntitySet( String entitySetName ) {
        try{
            modelService.deleteEntitySet( entitySetName );
        } catch (IllegalStateException e){
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() ); 
        }
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
        method = RequestMethod.PUT )
    @ResponseStatus( HttpStatus.OK )
    public Void putEntityType( @RequestBody EntityType entityType ) {
        try{
            modelService.createEntityType( Optional.fromNullable( authzService.getUserId() ), entityType );
        } catch ( IllegalArgumentException e){
            throw new BadRequestException( e.getMessage() );
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
            @RequestBody Set<FullQualifiedName> entityTypes ) {
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
            @RequestBody Set<FullQualifiedName> objectTypes ) {
        modelService.removeEntityTypesFromSchema( namespace, name, objectTypes );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void postEntityType( @RequestBody EntityType objectType ) {
        modelService.createEntityType( Optional.fromNullable( authzService.getUserId() ), objectType );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_TYPE_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public EntityType getEntityType( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        try{
            return modelService.getEntityType( namespace, name );
        } catch ( NullPointerException e ){
            throw new ResourceNotFoundException( e.getMessage() );
        }
    }

    @Override
    @RequestMapping(
        path = "/" + ENTITY_TYPE_BASE_PATH + "/" + NAMESPACE_PATH + "/" + NAME_PATH,
        method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void deleteEntityType( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        try{
            FullQualifiedName entityTypeFqn = new FullQualifiedName( namespace, name );
            modelService.deleteEntityType( entityTypeFqn );
        } catch ( IllegalStateException e ){
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() ); 
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void createPropertyType( @RequestBody PropertyType propertyType ) {
        try{
            modelService.createPropertyType( propertyType );
        } catch( IllegalStateException e ){
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() ); 
        }
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
    public Void putPropertyType( @RequestBody PropertyType propertyType ) {
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
        try{
            FullQualifiedName propertyTypeFqn = new FullQualifiedName( namespace, name );
            modelService.deletePropertyType( propertyTypeFqn );
        } catch ( IllegalStateException e ){
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() ); 
        }
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
        try{
            return modelService.getPropertyType( new FullQualifiedName( namespace, name ) );
        } catch ( NullPointerException e ){
            throw new ResourceNotFoundException( "Property type not found." ); 
        }
    }

    @Override
    @RequestMapping(
        path = "/" + PROPERTY_TYPE_BASE_PATH + "/" + NAMESPACE_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PropertyType> getPropertyTypesInNamespace( @PathVariable( NAMESPACE ) String namespace ) {
        try{
            return modelService.getPropertyTypesInNamespace( namespace );
        } catch( NullPointerException e ){
            throw new ResourceNotFoundException( "Property type not found." ); 
        }
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
            @RequestBody Set<FullQualifiedName> properties ) {
        try{
            modelService.addPropertyTypesToEntityType( namespace, name, properties );
        } catch ( IllegalArgumentException e ){
            throw new BadRequestException( e.getMessage() ); 
        }
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
            @RequestBody Set<FullQualifiedName> properties ) {
        try{
            modelService.removePropertyTypesFromEntityType( namespace, name, properties );
        } catch ( IllegalStateException e ){
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() ); 
        }
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
            @RequestBody Set<FullQualifiedName> properties ) {
        try{
            modelService.addPropertyTypesToSchema( namespace, name, properties );
        } catch ( IllegalStateException e ){
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() ); 
        }        
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
            @RequestBody Set<FullQualifiedName> properties ) {
        try{
            modelService.removePropertyTypesFromSchema( namespace, name, properties );
        } catch ( IllegalStateException e ){
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

}
