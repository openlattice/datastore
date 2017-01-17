package com.dataloom.organizations;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.dataloom.datastore.authentication.AuthenticatedRestCallsTest;
import com.dataloom.edm.internal.TestDataFactory;
import com.dataloom.organization.Organization;
import com.dataloom.organization.OrganizationsApi;

public class OrganizationsTest extends AuthenticatedRestCallsTest {
    private final OrganizationsApi organizations = getApi( OrganizationsApi.class );

    @Test
    public void getOrganizations() {
        
    }
    @Test
    public void createOrganizationTest() {
        UUID orgId = organizations.createOrganizationIfNotExists( TestDataFactory.organization() );
        Assert.assertNotNull( orgId );
        Organization organization = organizations.getOrganization( orgId );
        Assert.assertNotNull( organization );
    }
    
    @Test
    public void add() {
    }
}
