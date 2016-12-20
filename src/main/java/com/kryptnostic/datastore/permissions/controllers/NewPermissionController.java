package com.kryptnostic.datastore.permissions.controllers;

import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dataloom.authorization.AclData;
import com.dataloom.authorization.AclKeyInfo;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.NewPermissionsApi;
import com.kryptnostic.datastore.services.EdmService;

@RestController
@RequestMapping( NewPermissionsApi.PERMISSIONS )
public class NewPermissionController implements NewPermissionsApi {

    @Inject
    private AuthorizationManager authorizations;
    @Inject
    private EdmService           edmService;

    @Override
    public Map<String, AclKeyInfo> getAdministerableEntitySets() {
        // edmService.getEntitySets()
        // authorizations.
        // authorizations.getAllSecurableObjectPermissions( key );
        return null;
    }

    @Override
    public Set<AclData> getAcl( AclData request ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<AclData> updateAcl( AclData request ) {
        // TODO Auto-generated method stub
        return null;
    }

}
