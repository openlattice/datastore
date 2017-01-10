package com.kryptnostic.datastore.permissions.controllers;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpServerErrorException;

import com.dataloom.authorization.Acl;
import com.dataloom.authorization.AclData;
import com.dataloom.authorization.AclKeyPathFragment;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.PermissionsApi;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.Principals;
import com.kryptnostic.datastore.exceptions.ForbiddenException;

@RestController
public class PermissionsController implements PermissionsApi {
    private static final Logger  logger = LoggerFactory.getLogger( PermissionsController.class );
    @Inject
    private AuthorizationManager authorizations;

    @Override
    @RequestMapping(
        path = "/" + PERMISSIONS,
        method = RequestMethod.PATCH,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Void updateAcl( @RequestBody AclData req ) {
        /*
         * Ensure that the user has alter permissions on Acl permissions being modified
         */

        final Acl acl = req.getAcl();
        final List<AclKeyPathFragment> aclKeys = acl.getAclKey();
        if ( isOwnerOrCanAlter( aclKeys ) ) {
            switch ( req.getAction() ) {
                case ADD:
                    acl.getAces().forEach(
                            ace -> authorizations.addPermission(
                                    aclKeys,
                                    ace.getPrincipal(),
                                    ace.getPermissions() ) );
                    break;
                case REMOVE:
                    acl.getAces().forEach(
                            ace -> authorizations.removePermission(
                                    aclKeys,
                                    ace.getPrincipal(),
                                    ace.getPermissions() ) );
                    break;
                case SET:
                    acl.getAces().forEach(
                            ace -> authorizations.setPermission(
                                    aclKeys,
                                    ace.getPrincipal(),
                                    ace.getPermissions() ) );
                    break;
                default:
                    logger.error( "Invalid action {} specified for request.", req.getAction() );
                    throw new HttpServerErrorException( HttpStatus.BAD_REQUEST, "Invalid action specified." );
            }

        }
        throw new ForbiddenException( "Only owner of a securable object can access other users' access rights." );
    }

    @Override
    @RequestMapping(
        path = "/" + PERMISSIONS,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Acl getAcl( List<AclKeyPathFragment> aclKeys ) {
        return authorizations.getAllSecurableObjectPermissions( aclKeys );
    }

    private boolean isOwnerOrCanAlter( List<AclKeyPathFragment> aclKeys ) {
        Set<Principal> principals = Principals.getCurrentPrincipals();
        return authorizations.checkIfHasPermissions( aclKeys, principals, EnumSet.of( Permission.OWNER ) )
                || authorizations.checkIfHasPermissions( aclKeys, principals, EnumSet.of( Permission.ALTER ) );

    }
}
