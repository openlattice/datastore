package com.kryptnostic.datastore.permissions.controllers;

import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.kryptnostic.datastore.services.ActionAuthorizationService;
import com.kryptnostic.datastore.services.DataService;
import com.kryptnostic.datastore.services.PermissionsApi;
import com.kryptnostic.datastore.services.PermissionsService;
import com.kryptnostic.datastore.services.requests.Action;
import com.kryptnostic.datastore.services.requests.DeriveEntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.DerivePropertyTypeInEntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.DerivePropertyTypeInEntityTypeAclRequest;
import com.kryptnostic.datastore.services.requests.EntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntitySetAclRemovalRequest;
import com.kryptnostic.datastore.services.requests.EntitySetAclRemovalRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntityTypeAclRemovalRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntityTypeAclRequest;
import com.kryptnostic.datastore.services.requests.AclRequest;

import retrofit.client.Response;
import retrofit.http.POST;

@RestController
public class PermissionsController implements PermissionsApi {
    
    @Inject
    private PermissionsService ps;
    
    @Inject
    private ActionAuthorizationService authzService;
/**
 * TODO tighten up permissions, by first retrieving user role to see if they can do the corresponding actions. This should be done via authzService
 */
    
    @Override
    @RequestMapping( 
            path = CONTROLLER + ENTITY_TYPE_BASE_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response updateEntityTypesAcls( @RequestBody Set<AclRequest> requests) {
        for (AclRequest request : requests){
            switch ( request.getAction() ){
                case ADD:
                    ps.addPermissionsForEntityType( request.getRole(), request.getFqn(), request.getPermissions() );
                    break;
                case SET:
                    ps.setPermissionsForEntityType( request.getRole(), request.getFqn(), request.getPermissions() );
                    break;
                case REMOVE:
                    ps.removePermissionsForEntityType( request.getRole(), request.getFqn(), request.getPermissions() );
                    break;
            }
        }
        return null;
    }
    
    @Override
    @RequestMapping( 
            path = CONTROLLER + ENTITY_SETS_BASE_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response updateEntitySetsAcls( @RequestBody Set<EntitySetAclRequest> requests) {
        for (EntitySetAclRequest request : requests){
            switch ( request.getAction() ){
                case ADD:
                    ps.addPermissionsForEntitySet( request.getRole(), request.getFqn(), request.getEntitySetName(), request.getPermissions() );
                    break;
                case SET:
                    ps.setPermissionsForEntitySet( request.getRole(), request.getFqn(), request.getEntitySetName(), request.getPermissions() );
                    break;
                case REMOVE:
                    ps.removePermissionsForEntitySet( request.getRole(), request.getFqn(), request.getEntitySetName(), request.getPermissions() );
                    break;
            }
        }
        return null;
    }

    @Override
    @RequestMapping( 
            path = CONTROLLER + ENTITY_TYPE_BASE_PATH + PROPERTY_TYPE_BASE_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response updatePropertyTypeInEntityTypeAcls( @RequestBody  Set<PropertyTypeInEntityTypeAclRequest> requests) {
        for (PropertyTypeInEntityTypeAclRequest request : requests){
            switch ( request.getAction() ){
                case ADD:
                    ps.addPermissionsForPropertyTypeInEntityType( request.getRole(), request.getFqn(), request.getPropertyType(), request.getPermissions() );
                    break;
                case SET:
                    ps.setPermissionsForPropertyTypeInEntityType( request.getRole(), request.getFqn(), request.getPropertyType(), request.getPermissions() );
                    break;
                case REMOVE:
                    ps.removePermissionsForPropertyTypeInEntityType( request.getRole(), request.getFqn(), request.getPropertyType(), request.getPermissions() );
                    break;
            }
        }
        return null;
    }

    @Override
    @RequestMapping( 
            path = CONTROLLER + ENTITY_SETS_BASE_PATH + PROPERTY_TYPE_BASE_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response updatePropertyTypeInEntitySetAcls(@RequestBody  Set<PropertyTypeInEntitySetAclRequest> requests) {
        for (PropertyTypeInEntitySetAclRequest request : requests){
            switch ( request.getAction() ){
                case ADD:
                    ps.addPermissionsForPropertyTypeInEntitySet( request.getRole(), request.getFqn(), request.getName(), request.getPropertyType(), request.getPermissions() );
                    break;
                case SET:
                    ps.setPermissionsForPropertyTypeInEntitySet( request.getRole(), request.getFqn(), request.getName(), request.getPropertyType(), request.getPermissions() );
                    break;
                case REMOVE:
                    ps.removePermissionsForPropertyTypeInEntitySet( request.getRole(), request.getFqn(), request.getName(), request.getPropertyType(), request.getPermissions() );
                    break;
            }
        }
        return null;
    }

    @Override
    @RequestMapping( 
            path = CONTROLLER + ENTITY_TYPE_BASE_PATH,
            method = RequestMethod.DELETE,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response removeEntityTypeAcls( @RequestBody  Set<FullQualifiedName> entityTypeFqns) {
        for (FullQualifiedName entityTypeFqn : entityTypeFqns){
            ps.removePermissionsForEntityType( entityTypeFqn );
        }
        return null;
    }
    
    @Override
    @RequestMapping( 
            path = CONTROLLER + ENTITY_SETS_BASE_PATH,
            method = RequestMethod.DELETE,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response removeEntitySetAcls( @RequestBody  Set<EntitySetAclRemovalRequest> requests) {
        for (EntitySetAclRemovalRequest request : requests){
            ps.removePermissionsForEntitySet( request.getType(), request.getName() );
        }
        return null;
    }
    
    @Override
    @RequestMapping( 
            path = CONTROLLER + ENTITY_TYPE_BASE_PATH + PROPERTY_TYPE_BASE_PATH,
            method = RequestMethod.DELETE,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response removePropertyTypeInEntityTypeAcls( @RequestBody  Set<PropertyTypeInEntityTypeAclRemovalRequest> requests) {
        for (PropertyTypeInEntityTypeAclRemovalRequest request : requests){
            for( FullQualifiedName propertyTypeFqn : request.getProperties() ){
                ps.removePermissionsForPropertyTypeInEntityType( request.getType(), propertyTypeFqn );
            }
        }
        return null;
    }
    
    @Override
    @RequestMapping( 
            path = CONTROLLER + ENTITY_TYPE_BASE_PATH + PROPERTY_TYPE_BASE_PATH + ALL_PATH,
            method = RequestMethod.DELETE,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response removeAllPropertyTypesInEntityTypeAcls( @RequestBody Set<FullQualifiedName> entityTypeFqns) {
        for ( FullQualifiedName entityTypeFqn : entityTypeFqns ){
            ps.removePermissionsForPropertyTypeInEntityType( entityTypeFqn );
        }
        return null;
    }
    
    @Override
    @RequestMapping( 
            path = CONTROLLER + ENTITY_SETS_BASE_PATH + PROPERTY_TYPE_BASE_PATH,
            method = RequestMethod.DELETE,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response removePropertyTypeInEntitySetAcls( @RequestBody  Set<PropertyTypeInEntitySetAclRemovalRequest> requests) {
        for (PropertyTypeInEntitySetAclRemovalRequest request : requests){
            for( FullQualifiedName propertyTypeFqn : request.getProperties() ){
                ps.removePermissionsForPropertyTypeInEntitySet( request.getType(), request.getName(), propertyTypeFqn );
            }
        }
        return null;
    }
    
    @Override
    @RequestMapping( 
            path = CONTROLLER + ENTITY_SETS_BASE_PATH + PROPERTY_TYPE_BASE_PATH + ALL_PATH,
            method = RequestMethod.DELETE,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response removeAllPropertyTypesInEntitySetAcls( @RequestBody Set<EntitySetAclRemovalRequest> requests) {
        for ( EntitySetAclRemovalRequest request : requests ){
            ps.removePermissionsForPropertyTypeInEntitySet( request.getType(), request.getName() );
        }
        return null;
    }    
}
