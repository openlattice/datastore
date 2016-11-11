package com.kryptnostic.datastore.services;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import retrofit.RequestInterceptor;
import retrofit.RestAdapter;

import java.util.List;
import java.util.Map;

public class UserDirectoryService {

    private static final Logger logger = LoggerFactory.getLogger( UserDirectoryService.class );

    private RestAdapter        adapter;
    private Auth0ManagementApi auth0ManagementApi;

    public UserDirectoryService( String token ) {
        adapter = new RestAdapter.Builder()
                .setEndpoint( "https://loom.auth0.com/api/v2" )
                .setRequestInterceptor(
                        (RequestInterceptor) facade -> {
                            facade.addHeader( "Authorization", token );
                            facade.addHeader( "Content-Type", MediaType.APPLICATION_JSON_VALUE );
                        } )
                .setLogLevel( RestAdapter.LogLevel.FULL )
                .setLog( msg -> logger.debug( msg.replaceAll( "%", "[percent]" ) ) )
                .build();
        auth0ManagementApi = adapter.create( Auth0ManagementApi.class );
    }

    public Map<String, Auth0UserBasic> getAllUsers() {
        Map<String, Auth0UserBasic> res = Maps.newHashMap();
        JSONObject[] response = auth0ManagementApi.getAllUsers();

        for ( int i = 0; i < response.length; i++ ) {
            String userId = (String) response[ i ].get( "user_id" );
            String email = (String) response[ i ].get( "email" );
            Map<Object, List<String>> app_metadata = (Map<Object, List<String>>) response[ i ].get( "app_metadata" );
            if ( app_metadata == null ) {
                app_metadata = Maps.newHashMap();
            }
            res.put( userId,
                    new Auth0UserBasic( userId, email, app_metadata.getOrDefault( "roles", Lists.newArrayList() ) ) );
        }
        return res;
    }

    public Auth0UserBasic getUser( String userId ) {
        JSONObject response = auth0ManagementApi.getUser( userId );
        String user_id = (String) response.get( "user_id" );
        String email = (String) response.get( "email" );
        Map<Object, List<String>> app_metadata = (Map<Object, List<String>>) response.get( "app_metadata" );
        if ( app_metadata == null ) {
            app_metadata = Maps.newHashMap();
        }
        return new Auth0UserBasic( userId, email, app_metadata.getOrDefault( "roles", Lists.newArrayList() ) );
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
        JSONObject app_metadata = new JSONObject();
        JSONObject roles = new JSONObject();
        roles.put( "roles", roleList );
        app_metadata.put( "app_metadata", roles );
        auth0ManagementApi.resetRolesOfUser( userId, app_metadata );
    }

}
