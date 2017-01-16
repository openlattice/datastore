package com.kryptnostic.datastore.requests.controllers;

import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.Principals;
import com.dataloom.requests.AclRootRequestDetailsPair;
import com.dataloom.requests.AclRootStatusPair;
import com.dataloom.requests.PermissionsRequest;
import com.dataloom.requests.PermissionsRequestsApi;
import com.dataloom.requests.PermissionsRequestsManager;
import com.dataloom.requests.RequestStatus;
import com.google.common.collect.Iterables;

@RestController
public class PermissionsRequestsController implements PermissionsRequestsApi, AuthorizingComponent {

    @Inject
    private AuthorizationManager      authorizations;

    @Inject
    private PermissionsRequestsManager prm;

    @Override
    @RequestMapping(
        path = "/" + PERMISSIONS,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void upsertRequest( AclRootRequestDetailsPair req ) {
        prm.upsertRequest( req.getAclRoot(), Principals.getCurrentUser(), req.getDetails().getPermissions() );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + PERMISSIONS + "/" + UNRESOLVED,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public PermissionsRequest getUnresolvedRequestOfUser( List<UUID> aclRoot ) {
        return prm.getUnresolvedRequestOfUser( aclRoot, Principals.getCurrentUser() );
    }

    @Override
    @RequestMapping(
        path = "/" + PERMISSIONS + "/" + RESOLVED,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PermissionsRequest> getResolvedRequestsOfUser( List<UUID> aclRoot ) {
        return prm.getResolvedRequestsOfUser( aclRoot, Principals.getCurrentUser() );
    }

    @Override
    @RequestMapping(
        path = "/" + PERMISSIONS + "/" + ADMIN,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void updateUnresolvedRequestStatus( PermissionsRequest req ) {
        ensureOwnerAccess( req.getAclRoot() );
        prm.updateUnresolvedRequestStatus( req.getAclRoot(),
                req.getUser(),
                req.getDetails().getStatus() );
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + PERMISSIONS + "/" + ADMIN + "/" + UNRESOLVED,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PermissionsRequest> getAllUnresolvedRequestsOfAdmin( AclRootStatusPair req ) {
        EnumSet<RequestStatus> status = req.getStatus() == null ? EnumSet.allOf( RequestStatus.class )
                : req.getStatus();

        if ( req.getAclRoot() != null ) {
            ensureOwnerAccess( req.getAclRoot() );
            if( status.equals( EnumSet.allOf( RequestStatus.class ) ) ){
                return prm.getAllUnresolvedRequestsOfAdmin( req.getAclRoot() );
            } else {
                return prm.getAllUnresolvedRequestsOfAdmin( req.getAclRoot(), status );
            }
        } else {
            Iterable<Iterable<PermissionsRequest>> requests;
            if( status.equals( EnumSet.allOf( RequestStatus.class ) ) ){
                requests = Iterables.transform(
                        authorizations.getAuthorizedObjects( Principals.getCurrentPrincipals(),
                                EnumSet.of( Permission.OWNER ) ),
                        aclRoot -> prm.getAllUnresolvedRequestsOfAdmin( aclRoot ) );
            } else {
                requests = Iterables.transform(
                        authorizations.getAuthorizedObjects( Principals.getCurrentPrincipals(),
                                EnumSet.of( Permission.OWNER ) ),
                        aclRoot -> prm.getAllUnresolvedRequestsOfAdmin( aclRoot, status ) );
            }
            return Iterables.concat( requests );
        }
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }
}
