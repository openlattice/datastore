package com.kryptnostic.datastore.permissions.controllers;

import java.util.Set;
import java.util.UUID;

import com.kryptnostic.datastore.services.PermissionsApi;
import com.kryptnostic.datastore.services.requests.AclRequest;

import retrofit.client.Response;

public class PermissionsController implements PermissionsApi {

    @Override
    public Response setPropertyTypeAcls( String namespace, String name, Set<AclRequest> requests ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Response removePropertyTypeAcls( String namespace, String name, Set<UUID> users ) {
        // TODO Auto-generated method stub
        return null;
    }

}
