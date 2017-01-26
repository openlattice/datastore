package com.dataloom.datastore.authentication;

import com.dataloom.datastore.BootstrapDatastoreWithCassandra;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.internal.EntityType;
import org.junit.Assert;
import org.junit.Test;

public class AuthenticatedRestCallsTest extends BootstrapDatastoreWithCassandra {

    @Test
    public void testAuthenticatedAPICall() {
        Iterable<EntityType> entityTypes = getApiAdmin( EdmApi.class ).getEntityTypes();
        Assert.assertNotNull( entityTypes );
    }

}
