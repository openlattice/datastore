package com.dataloom.organizations;

import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dataloom.authorization.Principal;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organization.roles.OrganizationRole;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class RolesServiceTest extends OrganizationsTest {
    private static UUID organizationId;
    
    @BeforeClass
    public static void init() {        
        organizationId = organizations.createOrganizationIfNotExists( TestDataFactory.organization() );
    }
    
    private OrganizationRole createRole( UUID organizationId ){
        OrganizationRole role = new OrganizationRole(
                Optional.absent(),
                organizationId,
                RandomStringUtils.randomAlphanumeric( 5 ),
                Optional.of( RandomStringUtils.randomAlphanumeric( 5 ) )
                );
        UUID roleId = organizations.createRole( role );
        Assert.assertNotNull( roleId );
        OrganizationRole registered = organizations.getRole( organizationId, roleId );
        Assert.assertNotNull( registered );
        return registered;
    }
    
    @Test
    public void testCreateRole(){
        createRole( organizationId );
    }
    
    @Test
    public void testGetRoles(){
        int initialNumRoles = Iterables.size( organizations.getRoles( organizationId ) );
        
        OrganizationRole newRole = createRole( organizationId );
        
        Set<Principal> allRoles = ImmutableSet.copyOf( organizations.getRoles( organizationId ) );
        
        Assert.assertTrue( allRoles.contains( newRole.getPrincipal() ) );
        Assert.assertEquals( initialNumRoles + 1, allRoles.size() );
    }

    @Test
    public void testUpdateRoleTitle(){
        OrganizationRole newRole = createRole( organizationId );
        
        String newTitle = RandomStringUtils.randomAlphanumeric( 5 );
        organizations.updateRoleTitle( organizationId, newRole.getId(), newTitle );
        
        Assert.assertEquals( newTitle, organizations.getRole( organizationId, newRole.getId() ).getTitle() );
    }

    @Test
    public void testUpdateRoleDescription(){
        OrganizationRole newRole = createRole( organizationId );
        
        String newDescription = RandomStringUtils.randomAlphanumeric( 5 );
        organizations.updateRoleDescription( organizationId, newRole.getId(), newDescription );
        
        Assert.assertEquals( newDescription, organizations.getRole( organizationId, newRole.getId() ).getDescription() );
    }

    @Test
    public void testDeleteRole(){
        OrganizationRole newRole = createRole( organizationId );
        
        organizations.deleteRole( organizationId, newRole.getId() );
        
        Assert.assertFalse( Iterables.contains( organizations.getRoles( organizationId ), newRole.getPrincipal() ) );
    }
    
    @Test
    public void testAddRemoveRoleToUser(){
        OrganizationRole newRole = createRole( organizationId );
        
        organizations.addRoleToUser( organizationId, newRole.getId(), user2.getId() );

        Iterable<String> usersOfRoleAfterAdding = Iterables.transform( organizations.getAllUsersOfRole( organizationId, newRole.getId() ), Auth0UserBasic::getUserId );
        Assert.assertTrue( Iterables.contains( usersOfRoleAfterAdding, user2.getId() ) );
        
        organizations.removeRoleFromUser( organizationId, newRole.getId(), user2.getId() );

        Iterable<String> usersOfRoleAfterRemoving = Iterables.transform( organizations.getAllUsersOfRole( organizationId, newRole.getId() ), Auth0UserBasic::getUserId );
        Assert.assertFalse( Iterables.contains( usersOfRoleAfterRemoving, user2.getId() ) );
    }

}
