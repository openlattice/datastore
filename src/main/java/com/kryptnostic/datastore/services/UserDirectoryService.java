package com.kryptnostic.datastore.services;

import com.dataloom.client.RetrofitFactory;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.kryptnostic.datastore.exceptions.BadRequestException;
import org.json.simple.JSONObject;
import retrofit2.Retrofit;

import java.util.List;
import java.util.Map;

public class UserDirectoryService {

    private Retrofit           retrofit;
    private Auth0ManagementApi auth0ManagementApi;

    public UserDirectoryService( String token ) {
        retrofit = RetrofitFactory.newClient( "https://loom.auth0.com/api/v2/", () -> token );
        auth0ManagementApi = retrofit.create( Auth0ManagementApi.class );
    }

    public Map<String, Auth0UserBasic> getAllUsers() {
        Map<String, Auth0UserBasic> res = Maps.newHashMap();
        JSONObject[] jsonObjects = auth0ManagementApi.getAllUsers();

        for ( int i = 0; i < jsonObjects.length; i++ ) {
            Auth0UserBasic auth0UserBasic = parsingToPOJO( jsonObjects[ i ] );
            res.put( auth0UserBasic.getUserId(), auth0UserBasic );
        }
        return res;
    }

    public Auth0UserBasic getUser( String userId ) {
        JSONObject jsonObject = auth0ManagementApi.getUser( userId );
        if ( jsonObject == null ) {
            throw new BadRequestException( "User does not exist!" );
        }
        return parsingToPOJO( jsonObject );
    }

    private Auth0UserBasic parsingToPOJO( JSONObject jsonObject ) {
        String user_id = (String) jsonObject.get( "user_id" );
        String email = (String) jsonObject.get( "email" );
        String nickname = (String) jsonObject.get( "nickname" );
        Map<Object, List<String>> app_metadata = (Map<Object, List<String>>) jsonObject.get( "app_metadata" );
        if ( app_metadata == null ) {
            app_metadata = Maps.newHashMap();
        }
        return new Auth0UserBasic( user_id,
                email,
                nickname,
                app_metadata.getOrDefault( "roles", Lists.newArrayList() ) );
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
