package com.kryptnostic.datastore.permissions.controllers;

import java.util.List;

import javax.inject.Inject;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.dataloom.authorization.Ace;
import com.dataloom.authorization.Acl;
import com.dataloom.authorization.AclData;
import com.dataloom.authorization.AclKey;
import com.dataloom.authorization.HazelcastAuthorizationService;
import com.dataloom.authorization.NewPermissionsApi;
import com.dataloom.authorization.Principals;
import com.kryptnostic.datastore.exceptions.ForbiddenException;

@RestController
public class NewPermissionsController implements NewPermissionsApi {

    @Inject
    private HazelcastAuthorizationService authz;

    @Override
    @RequestMapping(
        path = "/" + PERMISSIONS,
        method = RequestMethod.PATCH,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Acl updateAcl( AclData req ) {
        List<AclKey> aclKey = req.getAcl().getAclKey();
        if ( !authz.checkIfUserIsOwner( aclKey, Principals.getCurrentUser() ) ) {
            throw new ForbiddenException(
                    "Only owner of a securable object can modify other users' access rights." );
        }

        switch ( req.getAction() ) {
            case ADD:
                for ( Ace ace : req.getAcl().getAces() ) {
                    authz.addPermission( aclKey, ace.getPrincipal(), ace.getPermissions() );
                }
                break;
            case SET:
                for ( Ace ace : req.getAcl().getAces() ) {
                    authz.setPermission( aclKey, ace.getPrincipal(), ace.getPermissions() );
                }
                break;
            case REMOVE:
                for ( Ace ace : req.getAcl().getAces() ) {
                    authz.removePermission( aclKey, ace.getPrincipal(), ace.getPermissions() );
                }
                break;
            default:
                break;
        }
        return authz.getAllSecurableObjectPermissions( aclKey );
    }

    @Override
    @RequestMapping(
        path = "/" + PERMISSIONS,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Acl getAcl( List<AclKey> aclKey ) {
        if ( !authz.checkIfUserIsOwner( aclKey, Principals.getCurrentUser() ) ) {
            throw new ForbiddenException(
                    "Only owner of a securable object can access other users' access rights." );
        }
        return authz.getAllSecurableObjectPermissions( aclKey );
    }
}
