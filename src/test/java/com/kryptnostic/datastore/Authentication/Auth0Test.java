package com.kryptnostic.datastore.Authentication;

import java.util.UUID;

import javax.ws.rs.NotFoundException;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.auth0.Auth0;
import com.auth0.authentication.AuthenticationAPIClient;
import com.auth0.authentication.result.Authentication;
import com.auth0.authentication.result.Credentials;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.data.DataApi;
import com.dataloom.edm.EdmApi;
import com.google.common.collect.SetMultimap;
import com.kryptnostic.datastore.Datastore;

import digital.loom.rhizome.authentication.AuthenticationTest;
import digital.loom.rhizome.configuration.auth0.Auth0Configuration;
import retrofit2.Retrofit;

public class Auth0Test {
    private static final Logger                      logger = LoggerFactory.getLogger( Auth0Test.class );
    private static final Datastore                   ds     = new Datastore();
    private static Auth0Configuration                configuration;
    private static Auth0                             auth0;
    private static AuthenticationAPIClient           client;
    private static DataApi                           dataApi;
    private static Retrofit                          dataServiceRestAdapter;
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
        dataServiceRestAdapter = RetrofitFactory.newClient( Environment.TESTING, () -> jwtToken );
        dataApi = dataServiceRestAdapter.create( DataApi.class );
        edmApi = dataServiceRestAdapter.create( EdmApi.class );
    }

    @Test
    public void testAuthenticatedAPICall() {
        UUID entitySetId = null;
        try {
            entitySetId = edmApi.getEntitySetId( "employees" );
        } catch ( NotFoundException e ) {
            
        }

        try {
            Iterable<SetMultimap<FullQualifiedName, Object>> result = dataApi.getEntitySetData( UUID.randomUUID(), null );
            Assert.assertNull( result );
        } catch ( NotFoundException e ) {
            Assert.assertNull( entitySetId );
        }
    }

    @AfterClass
    public static void shutdown() throws Exception {
        ds.stop();
    }
}
