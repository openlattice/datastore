package com.dataloom.datastore.services;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.client.RetrofitFactory;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.kryptnostic.datastore.services.Auth0ManagementApi;

import retrofit2.Retrofit;

public class UserDirectoryService {
    private static final Logger logger = LoggerFactory.getLogger( UserDirectoryService.class );
    private Retrofit            retrofit;
    private Auth0ManagementApi  auth0ManagementApi;

    public UserDirectoryService( String token ) {
        retrofit = RetrofitFactory.newClient( "https://loom.auth0.com/api/v2/", () -> token );
        auth0ManagementApi = retrofit.create( Auth0ManagementApi.class );
    }

    public Map<String, Auth0UserBasic> getAllUsers() {
        Set<Auth0UserBasic> users = auth0ManagementApi.getAllUsers();

        if ( users == null ) {
            logger.warn( "Received null response from auth0" );
            return ImmutableMap.of();
        }

        return users
                .stream()
                .collect( Collectors.toMap( Auth0UserBasic::getUserId, Function.identity() ) );
    }

    public Auth0UserBasic getUser( String userId ) {
        return auth0ManagementApi.getUser( userId );
    }

    public Map<String, List<Auth0UserBasic>> getAllUsersGroupByRole() {
        Map<String, List<Auth0UserBasic>> res = Maps.newHashMap();
        getAllUsers().values().forEach( user -> {
            for ( String r : user.getRoles() ) {
                List<Auth0UserBasic> users = res.getOrDefault( r, Lists.newArrayList() );
                users.add( user );
                res.put( r, users );
            }
        } );
        return res;
    }

    public List<Auth0UserBasic> getAllUsersOfRole( String role ) {
        List<Auth0UserBasic> res = Lists.newArrayList();
        getAllUsers().values().forEach( user -> {
            if ( user.getRoles().contains( role ) ) {
                res.add( user );
            }

        } );
        return res;
    }

    public void resetRolesOfUser( String userId, List<String> roleList ) {
        auth0ManagementApi.resetRolesOfUser( userId,
                ImmutableMap.of( "app_metadata", ImmutableMap.of( "roles", roleList ) ) );
    }

}
