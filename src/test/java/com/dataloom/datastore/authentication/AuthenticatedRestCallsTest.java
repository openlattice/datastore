package com.dataloom.datastore.authentication;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.Edm;
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

public class AuthenticatedRestCallsTest extends BootstrapDatastoreWithCassandra {

    @Test
    public void testAuthenticatedAPICall() {
        Iterable<EntityType> entityTypes = getApiAdmin( EdmApi.class ).getEntityTypes();
        Assert.assertNotNull( entityTypes );
    }

}
