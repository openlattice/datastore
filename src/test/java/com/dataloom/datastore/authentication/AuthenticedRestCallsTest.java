package com.dataloom.datastore.authentication;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import com.auth0.authentication.result.Authentication;
import com.auth0.authentication.result.Credentials;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.datastore.BootstrapDatastoreWithCassandra;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.internal.EntityType;

import digital.loom.rhizome.authentication.AuthenticationTest;
import retrofit2.Retrofit;

public class AuthenticedRestCallsTest extends BootstrapDatastoreWithCassandra {
    private static Retrofit                          retrofit;
    protected static EdmApi                          edmApi;
    private static Pair<Credentials, Authentication> authPair;

    static {
        authPair = AuthenticationTest.authenticate();
        String jwtToken = authPair.getLeft().getIdToken();
        retrofit = RetrofitFactory.newClient( Environment.TESTING, () -> jwtToken );
        edmApi = getApi( EdmApi.class );
    }

    @Test
    public void testAuthenticatedAPICall() {
        Iterable<EntityType> entityTypes = edmApi.getEntityTypes();
        Assert.assertNotNull( entityTypes );
    }

    protected static <T> T getApi( Class<T> clazz ) {
        return retrofit.create( clazz );
    }
}
