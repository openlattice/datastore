package com.kryptnostic.datastore.Authentication;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.auth0.authentication.result.Authentication;
import com.auth0.authentication.result.Credentials;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.internal.EntityType;
import com.kryptnostic.datastore.edm.BootstrapDatastoreWithCassandra;

import digital.loom.rhizome.authentication.AuthenticationTest;
import retrofit2.Retrofit;

public class Auth0Test extends BootstrapDatastoreWithCassandra {
    private static final Logger                      logger = LoggerFactory.getLogger( Auth0Test.class );
    private static Retrofit                          dataServiceRestAdapter;
    protected static EdmApi                          edmApi;
    private static Pair<Credentials, Authentication> authPair;

    @BeforeClass
    public static void authInit() throws Exception {
        authPair = AuthenticationTest.authenticate();
        String jwtToken = authPair.getLeft().getIdToken();
        dataServiceRestAdapter = RetrofitFactory.newClient( Environment.TESTING, () -> jwtToken );
        edmApi = dataServiceRestAdapter.create( EdmApi.class );
    }

    @Test
    public void testAuthenticatedAPICall() {
        Iterable<EntityType> entityTypes = edmApi.getEntityTypes();
        Assert.assertNotNull( entityTypes );
    }

}
