package com.kryptnostic.datastore.directory.controllers;

import com.kryptnostic.datastore.services.Auth0UserBasic;
import com.kryptnostic.datastore.services.UserDirectoryApi;
import com.kryptnostic.datastore.services.UserDirectoryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import retrofit.client.Response;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping( UserDirectoryApi.CONTROLLER )
public class UserDirectoryController implements UserDirectoryApi {

    @Inject
    private UserDirectoryService userDirectoryService;

    @Override
    @RequestMapping(
            path = UserDirectoryApi.USERS,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, Auth0UserBasic> getAllUsers() {
        return userDirectoryService.getAllUsers();
    }

    @Override
    @RequestMapping(
            path = UserDirectoryApi.USERS + UserDirectoryApi.USER_ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Auth0UserBasic getUser( @PathVariable( USER_ID ) String userId ) {
        return userDirectoryService.getUser( userId );
    }

    @Override
    @RequestMapping(
            path = UserDirectoryApi.USERS + UserDirectoryApi.ROLES,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, List<Auth0UserBasic>> getAllUsersGroupByRole() {
        return userDirectoryService.getAllUsersGroupByRole();
    }

    @Override
    @RequestMapping(
            path = UserDirectoryApi.USERS + UserDirectoryApi.ROLES + UserDirectoryApi.ROLE_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public List<Auth0UserBasic> getAllUsersOfRole(@PathVariable( ROLE ) String role){
        return userDirectoryService.getAllUsersOfRole( role );
    }

    @Override
    @RequestMapping(
            path = UserDirectoryApi.USERS + UserDirectoryApi.ROLES + UserDirectoryApi.RESET + UserDirectoryApi.USER_ID_PATH,
            method = RequestMethod.PATCH,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public void resetRolesOfUser( @PathVariable( USER_ID ) String userId, @RequestBody List<String> roles ) {
        userDirectoryService.resetRolesOfUser( userId, roles );
    }
}
