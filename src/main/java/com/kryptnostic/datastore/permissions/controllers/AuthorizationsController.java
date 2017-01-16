package com.kryptnostic.datastore.permissions.controllers;

import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.dataloom.authorization.AccessCheck;
import com.dataloom.authorization.Authorization;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizationsApi;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principals;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

public class AuthorizationsController implements AuthorizationsApi, AuthorizingComponent {
    private static final Logger  logger = LoggerFactory.getLogger( AuthorizationsController.class );

    @Inject
    private AuthorizationManager authorizations;

    @Override
    @RequestMapping(
        path = "/" + AUTHORIZATIONS,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Authorization> checkAuthorizations( Set<AccessCheck> queries ) {
        return Iterables.transform( queries, this::getAuth )::iterator;
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

    private Authorization getAuth( AccessCheck query ) {
        Set<Permission> currentPermissions = authorizations.getSecurableObjectPermissions( query.getAclKey(),
                Principals.getCurrentPrincipals() );
        Map<Permission, Boolean> permissionsMap = Maps.asMap( query.getPermissions(), currentPermissions::contains );
        return new Authorization( query.getAclKey(), permissionsMap );
    }
}
