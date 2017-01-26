package com.dataloom.datastore.authentication;

import java.util.HashMap;
import java.util.Map;

import com.auth0.authentication.result.Authentication;
import com.dataloom.authorization.AuthorizationsApi;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.datastore.BootstrapDatastoreWithCassandra;
import com.dataloom.edm.EdmApi;
import com.dataloom.requests.RequestsApi;

import digital.loom.rhizome.authentication.AuthenticationTest;
import digital.loom.rhizome.authentication.AuthenticationTestRequestOptions;
import retrofit2.Retrofit;

public class AuthenticationTestBase extends BootstrapDatastoreWithCassandra {
    protected static Map<String, AuthenticationTestRequestOptions> authMap = new HashMap<>();

    protected static Retrofit retrofit;
    protected static EdmApi edmApi;
    protected static RequestsApi requestsApi;
    protected static AuthorizationsApi authorizationsApi;
    
    static{
        authMap.put( "support", new AuthenticationTestRequestOptions()
                .setUsernameOrEmail( "support@kryptnostic.com" )
                .setPassword( "abracadabra" ) );
        authMap.put( "dummyuser", new AuthenticationTestRequestOptions()
                .setUsernameOrEmail( "test+dummyuser@kryptnostic.com" )
                .setPassword( "dummyuser12345" ) );
    }
    
    protected static void loginAs( String user ){
        AuthenticationTestRequestOptions loginDetails = authMap.get( user );
        if( loginDetails == null ){
            throw new IllegalArgumentException( "User doesn't exist." );
        }
        Authentication auth = AuthenticationTest.getAuthentication( loginDetails );
        String jwtToken = auth.getCredentials().getIdToken();
        retrofit = RetrofitFactory.newClient( Environment.TESTING, () -> jwtToken );
        
        System.out.println( "Retrofit baseUrl: " + retrofit.baseUrl() );
        edmApi = getApi( EdmApi.class );
        requestsApi = getApi( RequestsApi.class );
        authorizationsApi = getApi( AuthorizationsApi.class );
        
        System.out.println( "Logged in as: " + user );
    }
    
    protected static <T> T getApi( Class<T> clazz ) {
        return retrofit.create( clazz );
    }
}
