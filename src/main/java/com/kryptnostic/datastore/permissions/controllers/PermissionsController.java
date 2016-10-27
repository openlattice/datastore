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
    public Response updatePropertyTypeAcls( Set<AclRequest> requests) {
        for (AclRequest request : requests){
            switch ( request.getAction() ){
                case ADD:
                    ps.addPermissionsForPropertyType( request.getRole(), request.getFqn(), request.getPermissions() );
                    break;
                case SET:
                    ps.setPermissionsForPropertyType( request.getRole(), request.getFqn(), request.getPermissions() );
                    break;
                case REMOVE:
                    ps.removePermissionsForPropertyType( request.getRole(), request.getFqn(), request.getPermissions() );
                    break;
            }
        }
        return null;
    }


}
