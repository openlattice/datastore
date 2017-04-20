package com.dataloom.organizations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.Principal;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.organization.OrganizationsApi;
import com.dataloom.organization.roles.OrganizationRole;
import com.dataloom.organizations.roles.RolesUtil;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import digital.loom.rhizome.authentication.AuthenticationTest;
import digital.loom.rhizome.authentication.AuthenticationTestRequestOptions;
import retrofit2.Retrofit;

public class RolesServiceTest extends OrganizationsTest {
    private static final Logger                                          logger         = LoggerFactory
            .getLogger( RolesServiceTest.class );

    private static UUID                                                  organizationId;
    protected static final Map<String, AuthenticationTestRequestOptions> authOptionsMap = new HashMap<>();
    protected static final Map<String, Retrofit>                         retrofitMap    = new HashMap<>();

    static {
        authOptionsMap.put( "user1", authOptions1 );
        authOptionsMap.put( "user2", authOptions2 );
        authOptionsMap.put( "user3", authOptions3 );
        retrofitMap.put( "user1", retrofit1 );
        retrofitMap.put( "user2", retrofit2 );
        retrofitMap.put( "user3", retrofit3 );
    }

    @BeforeClass
    public static void init() {
        organizationId = organizations.createOrganizationIfNotExists( TestDataFactory.organization() );
    }

    private OrganizationRole createRole( UUID organizationId ) {
        OrganizationRole role = new OrganizationRole(
                Optional.absent(),
                organizationId,
                RandomStringUtils.randomAlphanumeric( 5 ),
                Optional.of( RandomStringUtils.randomAlphanumeric( 5 ) ) );
        UUID roleId = organizations.createRole( role );
        Assert.assertNotNull( roleId );
        OrganizationRole registered = organizations.getRole( organizationId, roleId );
        Assert.assertNotNull( registered );
        return registered;
    }

    @Test
    public void testCreateRole() {
        createRole( organizationId );
    }

    @Test
    public void testGetRoles() {
        int initialNumRoles = Iterables.size( organizations.getRoles( organizationId ) );

        OrganizationRole newRole = createRole( organizationId );

        Set<Principal> allRoles = ImmutableSet.copyOf( organizations.getRoles( organizationId ) );

        Assert.assertTrue( allRoles.contains( RolesUtil.getPrincipal( newRole ) ) );
        Assert.assertEquals( initialNumRoles + 1, allRoles.size() );
    }

    @Test
    public void testUpdateRoleTitle() {
        OrganizationRole newRole = createRole( organizationId );

        String newTitle = RandomStringUtils.randomAlphanumeric( 5 );
        organizations.updateRoleTitle( organizationId, newRole.getId(), newTitle );

        Assert.assertEquals( newTitle, organizations.getRole( organizationId, newRole.getId() ).getTitle() );
    }

    @Test
    public void testUpdateRoleDescription() {
        OrganizationRole newRole = createRole( organizationId );

        String newDescription = RandomStringUtils.randomAlphanumeric( 5 );
        organizations.updateRoleDescription( organizationId, newRole.getId(), newDescription );

        Assert.assertEquals( newDescription,
                organizations.getRole( organizationId, newRole.getId() ).getDescription() );
    }

    @Test
    public void testDeleteRole() {
        OrganizationRole newRole = createRole( organizationId );

        organizations.deleteRole( organizationId, newRole.getId() );

        Assert.assertFalse( Iterables.contains( organizations.getRoles( organizationId ), RolesUtil.getPrincipal( newRole ) ) );
    }

    @Test
    public void testAddRemoveRoleToUser() {
        OrganizationRole newRole = createRole( organizationId );

        organizations.addRoleToUser( organizationId, newRole.getId(), user2.getId() );

        Iterable<String> usersOfRoleAfterAdding = Iterables.transform(
                organizations.getAllUsersOfRole( organizationId, newRole.getId() ), Auth0UserBasic::getUserId );
        Assert.assertTrue( Iterables.contains( usersOfRoleAfterAdding, user2.getId() ) );

        organizations.removeRoleFromUser( organizationId, newRole.getId(), user2.getId() );

        Iterable<Auth0UserBasic> usersOfRoleAfterRemovingInFull = organizations.getAllUsersOfRole( organizationId, newRole.getId() );
        if( usersOfRoleAfterRemovingInFull == null ){
            usersOfRoleAfterRemovingInFull = new ArrayList<Auth0UserBasic>();
        }
        Iterable<String> usersOfRoleAfterRemoving = Iterables.transform(
                usersOfRoleAfterRemovingInFull, Auth0UserBasic::getUserId );
        Assert.assertFalse( Iterables.contains( usersOfRoleAfterRemoving, user2.getId() ) );
    }

    // TODO: Temporarily turn off manual token expiration
    @Ignore
    public void testRefreshToken() {
        // add role to user2
        OrganizationRole newRole = createRole( organizationId );
        organizations.addRoleToUser( organizationId, newRole.getId(), user2.getId() );

        Iterable<String> usersOfRoleAfterAdding = Iterables.transform(
                organizations.getAllUsersOfRole( organizationId, newRole.getId() ), Auth0UserBasic::getUserId );
        Assert.assertTrue( Iterables.contains( usersOfRoleAfterAdding, user2.getId() ) );

        logger.info( "Login as user2, anticipating a failed call with TokenRefreshException." );
        OrganizationsApi orgsApiOfUser2 = retrofit2.create( OrganizationsApi.class );
        // This call should fail with TokenRefreshException
        Assert.assertNull( orgsApiOfUser2.createOrganizationIfNotExists( TestDataFactory.organization() ) );

        logger.info( "Refreshed Login as user2, anticipating a success call." );
        // Update token
        OrganizationsApi refreshedOrgsApiOfUser2 = refreshOrganizationApi( "user2" );
        // This call should now succeed
        Assert.assertNotNull( refreshedOrgsApiOfUser2.createOrganizationIfNotExists( TestDataFactory.organization() ) );
    }

    /**
     * Utils
     */
    protected static OrganizationsApi refreshOrganizationApi( String user ) {
        AuthenticationTestRequestOptions authOption = authOptionsMap.get( user );
        if ( authOption == null ) {
            throw new IllegalArgumentException( "User does not exist in Retrofit map." );
        }
        String jwt = AuthenticationTest.refreshAndGetAuthentication( authOptionsMap.get( user ) ).getCredentials()
                .getIdToken();
        Retrofit r = RetrofitFactory.newClient( RetrofitFactory.Environment.TESTING, () -> jwt );

        return r.create( OrganizationsApi.class );
    }
}
