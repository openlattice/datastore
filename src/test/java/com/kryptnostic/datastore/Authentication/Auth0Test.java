package com.kryptnostic.datastore.Authentication;

import com.auth0.Auth0;
import com.auth0.authentication.AuthenticationAPIClient;
import com.auth0.authentication.result.Authentication;
import com.auth0.authentication.result.Credentials;
import com.auth0.authentication.result.UserProfile;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.internal.org.apache.commons.codec.binary.Base64;
import com.auth0.jwt.internal.org.apache.commons.lang3.tuple.Pair;
import com.auth0.request.AuthenticationRequest;
import com.auth0.spring.security.api.Auth0AuthorityStrategy;
import com.auth0.spring.security.api.Auth0JWTToken;
import com.auth0.spring.security.api.Auth0UserDetails;
import com.google.common.collect.ImmutableList;
import com.kryptnostic.datastore.Datastore;
import com.kryptnostic.datastore.edm.DatastoreServices;
import com.kryptnostic.datastore.services.DataApi;
import com.kryptnostic.kodex.v1.exceptions.DefaultErrorHandler;
import com.squareup.okhttp.Headers;
import com.squareup.okhttp.OkHttpClient;
import digital.loom.rhizome.configuration.auth0.Auth0Configuration;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import retrofit.RequestInterceptor;
import retrofit.RestAdapter;
import retrofit.client.OkClient;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Auth0Test {
    private static final Logger    logger = LoggerFactory.getLogger( Auth0Test.class );
    private static final Datastore ds     = new Datastore();
    private static Auth0Configuration      configuration;
    private static Auth0                   auth0;
    private static AuthenticationAPIClient client;
    private static DataApi  dataApi;

    @BeforeClass
    public static void init() throws Exception {
        ds.start( "cassandra" );
        configuration = ds.getContext().getBean( Auth0Configuration.class );
        auth0 = new Auth0( configuration.getClientId(), configuration.getDomain() );
        client = auth0.newAuthenticationAPIClient();

        // TODO: figure out this
        RestAdapter dataServiceRestAdapter = createRestAdapter( DataApi.CONTROLLER, new RequestInterceptor() {
            @Override public void intercept( RequestFacade request ) {
                request.addHeader( "access_token", "" );
                request.addHeader( "id_token", "" );
            }
        } );
        dataApi = dataServiceRestAdapter.create( DataApi.class );
    }

    protected static RestAdapter createRestAdapter( String urlSuffix, RequestInterceptor requestInterceptor ) {
        return createDefaultRestAdapterBuilder( urlSuffix ).setRequestInterceptor( requestInterceptor ).build();
    }

    protected static RestAdapter.Builder createDefaultRestAdapterBuilder( String urlSuffix ) {
        OkHttpClient client = createOkHttpClient();
        String url = "http://localhost:8080/ontology" + urlSuffix;
        return new RestAdapter.Builder()
                .setEndpoint( url )
                .setClient( new OkClient( client ) )
                .setErrorHandler( new DefaultErrorHandler() )
                .setLogLevel( RestAdapter.LogLevel.FULL )
                .setLog( new RestAdapter.Log() {
                    @Override
                    public void log( String msg ) {
                        logger.debug( msg );
                    }
                } );
    }

    protected static OkHttpClient createOkHttpClient() {
        OkHttpClient client = new OkHttpClient();
        client.setReadTimeout( 0, TimeUnit.MILLISECONDS );
        client.setConnectTimeout( 0, TimeUnit.MILLISECONDS );
        return client;
    }

    @Test
    public void testRoles() throws Exception {
        Pair<Credentials, Authentication> auth = authenticate();
        JWTVerifier jwtVerifier = new JWTVerifier( new Base64( true ).decodeBase64( configuration.getClientSecret() ),
                configuration.getClientId(),
                configuration.getIssuer() );
        Auth0JWTToken token = new Auth0JWTToken( auth.getLeft().getIdToken() );
        final Map<String, Object> decoded = jwtVerifier.verify( auth.getLeft().getIdToken() );
        Map<String, Object> d2 = auth.getRight().getProfile().getAppMetadata();
        Auth0UserDetails userDetails = new Auth0UserDetails(
                d2,
                Auth0AuthorityStrategy.valueOf( configuration.getAuthorityStrategy() ).getStrategy() );
        Assert.assertTrue( "Return roles must contain user",
                userDetails.getAuthorities().contains( new SimpleGrantedAuthority( "user" ) ));
        logger.info( "Roles: {}", userDetails.getAuthorities() );
    }

    @Test
    public void testLogin() {
        Credentials credentials = authenticate().getLeft();
        UserProfile profile = client.tokenInfo( credentials.getIdToken() ).execute();

        List<String> roles = (List<String>) profile.getAppMetadata().getOrDefault( "roles", ImmutableList.of() );
        Assert.assertTrue( "Return roles must contain user", roles.contains( "user" ) );
        Assert.assertTrue( StringUtils.isNotBlank( credentials.getIdToken() ) );
    }

    // TODO: implement this
    @Test(expected = AccessDeniedException.class)
    public void testUnauthenticatedAPICall(){
        throw new AccessDeniedException( "Please Login." );
    }

    // TODO: implement this
    @Test
    public void testAuthenticatedAPICall(){

    }

    private static Pair<Credentials, Authentication> authenticate() {
        AuthenticationRequest request = client.login( "support@kryptnostic.com", "abracadabra" )
                .setConnection( "Tests" );
        return Pair.of( request.execute(), client.getProfileAfter( request ).execute() );
    }

    @AfterClass
    public static void shutdown() throws Exception {
        ds.stop();
    }
}
