package com.kryptnostic.datastore.Authentication;

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
import com.google.common.collect.Multimap;
import com.kryptnostic.datastore.Datastore;

import digital.loom.rhizome.authentication.AuthenticationTest;
import digital.loom.rhizome.configuration.auth0.Auth0Configuration;
import retrofit2.Retrofit;

import javax.ws.rs.NotFoundException;

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
        boolean entityTypeExists = true;
        
        try{
            edmApi.getEntityType( "testcsv", "employee" );
        } catch ( NotFoundException e){
            entityTypeExists = false;
        }
        
        try{
            Iterable<Multimap<FullQualifiedName, Object>> result = dataApi.getAllEntitiesOfType( "testcsv", "employee" );
            Assert.assertNull( result );
        } catch ( NotFoundException e ){
            //NotFoundException only thrown if testcsv.employee entity Type doesn't exist.
            Assert.assertFalse( entityTypeExists );
        }
    }

    @AfterClass
    public static void shutdown() throws Exception {
        ds.stop();
    }
}
