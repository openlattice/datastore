/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.datastore.authorization.controllers;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dataloom.authorization.AccessCheck;
import com.dataloom.authorization.Authorization;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizationsApi;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principals;
import com.dataloom.authorization.paging.AuthorizedObjectsPagingFactory;
import com.dataloom.authorization.paging.AuthorizedObjectsSearchResult;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.edm.EdmApi.FileType;
import com.google.common.collect.Maps;

@RestController
@RequestMapping( AuthorizationsApi.CONTROLLER )
public class AuthorizationsController implements AuthorizationsApi, AuthorizingComponent {
    private static final Logger  logger = LoggerFactory.getLogger( AuthorizationsController.class );
    //Number of authorized objects in each page of results
    private static final int DEFAULT_PAGE_SIZE = 50;
    
    @Inject
    private AuthorizationManager authorizations;

    @Override
    @RequestMapping(
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Authorization> checkAuthorizations( @RequestBody Set<AccessCheck> queries ) {
        return queries.stream().map( this::getAuth )::iterator;
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
    
    @Override
    @RequestMapping(
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public AuthorizedObjectsSearchResult getAccessibleObjects( 
            @RequestParam(
                    value = OBJECT_TYPE,
                    required = true ) SecurableObjectType objectType,
            @RequestParam(
                    value = PERMISSION,
                    required = true ) Permission permission,
            @RequestParam(
                    value = PAGING_TOKEN,
                    required = false ) String pagingToken
            ){
        return authorizations.getAuthorizedObjectsOfType( Principals.getCurrentPrincipals(), objectType, permission, AuthorizedObjectsPagingFactory.decode( pagingToken ), DEFAULT_PAGE_SIZE );
    }    

}
