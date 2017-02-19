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

package com.dataloom.datastore.directory.controllers;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.dataloom.directory.PrincipalApi;
import com.dataloom.directory.UserDirectoryService;
import com.dataloom.directory.pojo.Auth0UserBasic;

@RestController
@RequestMapping( PrincipalApi.CONTROLLER )
public class PrincipalDirectoryController implements PrincipalApi {

    @Inject
    private UserDirectoryService userDirectoryService;

    @Override
    @RequestMapping(
        path = USERS,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, Auth0UserBasic> getAllUsers() {
        return userDirectoryService.getAllUsers();
    }

    @Override
    @RequestMapping(
        path = USERS + USER_ID_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Auth0UserBasic getUser( @PathVariable( USER_ID ) String userId ) {
        return userDirectoryService.getUser( userId );
    }

    @Override
    @RequestMapping(
        path = USERS + USER_ID_PATH + ROLES,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void setUserRoles( @PathVariable( USER_ID ) String userId, @RequestBody Set<String> roles ) {
        userDirectoryService.setRolesOfUser( userId, roles );
        return null;
    }

    @Override
    @PutMapping(
        path = USERS + USER_ID_PATH + ROLES + ROLE_PATH )
    @ResponseStatus( HttpStatus.OK )
    public Void addRoleToUser( @PathVariable( USER_ID ) String userId, @PathVariable( ROLE ) String role ) {
       userDirectoryService.addRoleToUser( userId, role );
        return null;
    }

    @Override
    @DeleteMapping(
        path = USERS + USER_ID_PATH + ROLES + ROLE_PATH )
    @ResponseStatus( HttpStatus.OK )
    public Void removeRoleFromUser( @PathVariable( USER_ID ) String userId, @PathVariable( ROLE ) String role ) {
        userDirectoryService.removeRoleFromUser( userId, role );
        return null;
    }

    @Override
    @RequestMapping(
        path = ROLES,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<String, List<Auth0UserBasic>> getAllUsersGroupByRole() {
        return userDirectoryService.getAllUsersGroupByRole();
    }

    @Override
    @RequestMapping(
        path = ROLES + ROLE_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public List<Auth0UserBasic> getAllUsersOfRole( @PathVariable( ROLE ) String role ) {
        return userDirectoryService.getAllUsersOfRole( role );
    }

    @Override
    @GetMapping(
        path = USERS + SEARCH + SEARCH_QUERY_PATH,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<String, Auth0UserBasic> searchAllUsers( @PathVariable( SEARCH_QUERY ) String searchQuery ) {
        return userDirectoryService.searchAllUsers( searchQuery );
    }
}
