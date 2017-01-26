package com.dataloom.datastore.authentication;

import org.junit.Assert;
import org.junit.Test;

import com.auth0.authentication.result.Authentication;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.datastore.BootstrapDatastoreWithCassandra;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.internal.EntityType;

import digital.loom.rhizome.authentication.AuthenticationTest;
import retrofit2.Retrofit;

public class AuthenticatedRestCallsTest extends BootstrapDatastoreWithCassandra {
    private static Retrofit       retrofit;
    protected static EdmApi       edmApi;
    private static Authentication auth;

    static {
        auth = AuthenticationTest.authenticate();
        String jwtToken = auth.getCredentials().getIdToken();
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
