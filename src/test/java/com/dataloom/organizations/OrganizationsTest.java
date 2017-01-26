package com.dataloom.organizations;

import java.util.Set;
import java.util.UUID;

import com.dataloom.datastore.BootstrapDatastoreWithCassandra;
import org.junit.Assert;
import org.junit.Test;

import com.dataloom.authorization.Principal;
import com.dataloom.datastore.authentication.AuthenticatedRestCallsTest;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organization.Organization;
import com.dataloom.organization.OrganizationsApi;
import com.google.common.collect.ImmutableSet;

public class OrganizationsTest extends BootstrapDatastoreWithCassandra {
    private final OrganizationsApi organizations = getApiUser1( OrganizationsApi.class );

    private Organization createOrganization() {
        UUID orgId = organizations.createOrganizationIfNotExists( TestDataFactory.organization() );
        Assert.assertNotNull( orgId );
        Organization organization = organizations.getOrganization( orgId );
        Assert.assertNotNull( organization );
        return organization;
    }

    @Test
    public void getOrganizations() {
        Iterable<Organization> iorgs = organizations.getOrganizations();
        Assert.assertNotNull( iorgs );
        Set<Organization> orgs = ImmutableSet.copyOf( iorgs );
        
        Organization organization = createOrganization();
        
        iorgs = organizations.getOrganizations();
        Assert.assertNotNull( iorgs );
        Assert.assertTrue( !orgs.contains( organization ) );
        
        orgs = ImmutableSet.copyOf( iorgs );
        Assert.assertNotNull( orgs );
        Assert.assertTrue( orgs.contains( organization ) );
    }

    @Test
    public void createOrganizationTest() {
        createOrganization();
    }

    @Test
    public void addPrincipal() {
        Organization org = createOrganization();
        Principal p = TestDataFactory.rolePrincipal();
        Set<Principal> ps = organizations.getPrincipals( org.getId() );
        Assert.assertNotNull( ps );
        Assert.assertFalse( ps.contains( p ) );
        organizations.addPrincipal( org.getId(), p.getType(), p.getId() );

        ps = organizations.getPrincipals( org.getId() );
        Assert.assertNotNull( ps );
        Assert.assertTrue( ps.contains( p ) );
    }
}
