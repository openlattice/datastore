package com.kryptnostic.datastore.Authentication;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.auth0.Auth0;
import com.auth0.authentication.AuthenticationAPIClient;
import com.auth0.authentication.result.Authentication;
import com.auth0.authentication.result.Credentials;
import com.auth0.authentication.result.UserProfile;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.internal.org.apache.commons.codec.binary.Base64;
import com.auth0.spring.security.api.Auth0AuthorityStrategy;
import com.auth0.spring.security.api.Auth0JWTToken;
import com.auth0.spring.security.api.Auth0UserDetails;
import com.dataloom.data.DataApi;
import com.dataloom.edm.EdmApi;
import com.geekbeast.rhizome.tests.bootstrap.DefaultErrorHandler;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.kryptnostic.datastore.Datastore;
import com.kryptnostic.rhizome.converters.RhizomeConverter;
import com.squareup.okhttp.OkHttpClient;

import digital.loom.rhizome.authentication.AuthenticationTest;
import digital.loom.rhizome.configuration.auth0.Auth0Configuration;
import retrofit.RequestInterceptor;
import retrofit.RestAdapter;
import retrofit.client.OkClient;

public class Auth0Test {
    private static final Logger                      logger = LoggerFactory.getLogger( Auth0Test.class );
    private static final Datastore                   ds     = new Datastore();
    private static Auth0Configuration                configuration;
    private static Auth0                             auth0;
    private static AuthenticationAPIClient           client;
    private static DataApi                           dataApi;
    private static RestAdapter                       dataServiceRestAdapter;
    protected static EdmApi                          edmApi;
    private static Pair<Credentials, Authentication> authPair;

    @BeforeClass
    public static void init() throws Exception {
        ds.start( "cassandra" );
        configuration = ds.getContext().getBean( Auth0Configuration.class );
        auth0 = new Auth0( configuration.getClientId(), configuration.getDomain() );
        client = auth0.newAuthenticationAPIClient();
        authPair = AuthenticationTest.authenticate();
        String jwtToken = authPair.getLeft().getIdToken();
        OkHttpClient httpClient = new OkHttpClient();
        httpClient.setConnectTimeout( 60, TimeUnit.SECONDS );
        httpClient.setReadTimeout( 60, TimeUnit.SECONDS );
        OkClient client = new OkClient( httpClient );

        dataServiceRestAdapter = new RestAdapter.Builder()
                .setEndpoint( "http://localhost:8080/datastore/ontology" )
                .setRequestInterceptor(
                        (RequestInterceptor) facade -> facade.addHeader( "Authorization", "Bearer " + jwtToken ) )
                .setConverter( new RhizomeConverter() )
                .setErrorHandler( new DefaultErrorHandler() )
                .setLogLevel( RestAdapter.LogLevel.FULL )
                .setLog( msg -> logger.debug( msg.replaceAll( "%", "[percent]" ) ) )
                .setClient( client )
                .build();
        dataApi = dataServiceRestAdapter.create( DataApi.class );
        edmApi = dataServiceRestAdapter.create( EdmApi.class );
    }

    @Test
    public void testRoles() throws Exception {
        JWTVerifier jwtVerifier = new JWTVerifier(
                new Base64( true ).decodeBase64( configuration.getClientSecret() ),
                configuration.getClientId(),
                configuration.getIssuer() );
        Auth0JWTToken token = new Auth0JWTToken( authPair.getLeft().getIdToken() );
        final Map<String, Object> decoded = jwtVerifier.verify( authPair.getLeft().getIdToken() );
        Map<String, Object> d2 = authPair.getRight().getProfile().getAppMetadata();
        Auth0UserDetails userDetails = new Auth0UserDetails(
                d2,
                Auth0AuthorityStrategy.valueOf( configuration.getAuthorityStrategy() ).getStrategy() );
        Assert.assertTrue( "Return roles must contain user",
                userDetails.getAuthorities().contains( new SimpleGrantedAuthority( "user" ) ) );
        logger.info( "Roles: {}", userDetails.getAuthorities() );
    }

    @Test
    public void testLogin() {
        Credentials credentials = authPair.getLeft();
        UserProfile profile = client.tokenInfo( credentials.getIdToken() ).execute();

        List<String> roles = (List<String>) profile.getAppMetadata().getOrDefault( "roles", ImmutableList.of() );
        Assert.assertTrue( "Return roles must contain user", roles.contains( "user" ) );
        Assert.assertTrue( StringUtils.isNotBlank( credentials.getIdToken() ) );
    }

    @Test(
        expected = AccessDeniedException.class )
    public void testUnauthenticatedAPICall() {
        RestAdapter unauthorizedAdapter = new RestAdapter.Builder()
                .setEndpoint( "http://localhost:8080/datastore/ontology" )
                .setRequestInterceptor(
                        (RequestInterceptor) facade -> facade
                                .addHeader( "Authorization", "Bearer " + "I am wrong token" ) )
                .setConverter( new RhizomeConverter() )
                .setErrorHandler( new DefaultErrorHandler() )
                .setLogLevel( RestAdapter.LogLevel.FULL )
                .setLog( msg -> logger.debug( msg.replaceAll( "%", "[percent]" ) ) )
                .build();
        DataApi unauthorizedApi = unauthorizedAdapter.create( DataApi.class );
        unauthorizedApi.getAllEntitiesOfType( "testcsv", "employee" );
    }

    @Test
    public void testAuthenticatedAPICall() {
        Iterable<Multimap<FullQualifiedName, Object>> result = dataApi.getAllEntitiesOfType( "testcsv", "employee" );
        Assert.assertNull( result );
    }

    @AfterClass
    public static void shutdown() throws Exception {
        ds.stop();
    }
}
