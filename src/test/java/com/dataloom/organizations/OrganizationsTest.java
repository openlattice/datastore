/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.organizations;

import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.datastore.BootstrapDatastoreWithCassandra;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organization.Organization;
import com.dataloom.organization.OrganizationsApi;
import com.dataloom.organizations.roles.RolesUtil;
import com.google.common.collect.ImmutableSet;

public class OrganizationsTest extends BootstrapDatastoreWithCassandra {
    protected static final OrganizationsApi organizations = getApiUser1( OrganizationsApi.class );

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
        Principal expectedP = new Principal(
                PrincipalType.ROLE,
                RolesUtil.getStringRepresentation( org.getId(), p.getId() ) );

        Set<Principal> ps = organizations.getPrincipals( org.getId() );
        Assert.assertNotNull( ps );
        Assert.assertFalse( ps.contains( p ) );
        organizations.addPrincipal( org.getId(), p.getType(), p.getId() );

        ps = organizations.getPrincipals( org.getId() );
        Assert.assertNotNull( ps );
        Assert.assertTrue( ps.contains( expectedP ) );
    }
}
