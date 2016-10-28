package com.kryptnostic.datastore.permissions.controllers;

import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.kryptnostic.datastore.services.ActionAuthorizationService;
import com.kryptnostic.datastore.services.DataService;
import com.kryptnostic.datastore.services.PermissionsApi;
import com.kryptnostic.datastore.services.PermissionsService;
import com.kryptnostic.datastore.services.requests.Action;
import com.kryptnostic.datastore.services.requests.DeriveEntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.DerivePropertyTypeInEntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.DerivePropertyTypeInEntityTypeAclRequest;
import com.kryptnostic.datastore.services.requests.EntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.EntitySetRemoveAclRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntityTypeAclRequest;
import com.kryptnostic.datastore.services.requests.AclRequest;

import retrofit.client.Response;

public class PermissionsController implements PermissionsApi {
    
    @Inject
    private PermissionsService ps;
    
    @Inject
    private ActionAuthorizationService authzService;
/**
 * TODO tighten up permissions, by first retrieving user role to see if they can do the corresponding actions. This should be done via authzService
 */
    
    @Override
    public Response updateEntityTypesAcls( Set<AclRequest> requests) {
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
    public Response updateEntitySetsAcls( Set<EntitySetAclRequest> requests) {
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
    public Response updatePropertyTypeInEntityTypeAcls( Set<PropertyTypeInEntityTypeAclRequest> requests) {
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
    public Response updatePropertyTypeInEntitySetAcls( Set<PropertyTypeInEntitySetAclRequest> requests) {
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
    public Response removeEntityTypeAcls( Set<FullQualifiedName> entityTypeFqns) {
        for (FullQualifiedName entityTypeFqn : entityTypeFqns){
            ps.removePermissionsForEntityType( entityTypeFqn );
        }
        return null;
    }
    
    @Override
    public Response removeEntitySetAcls( Set<EntitySetRemoveAclRequest> requests) {
        for (EntitySetRemoveAclRequest request : requests){
            ps.removePermissionsForEntitySet( request.getType(), request.getName() );
        }
        return null;
    }
}
