package com.dataloom.requests;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.auth0.jwt.internal.org.apache.commons.lang3.RandomStringUtils;
import com.dataloom.authorization.AccessCheck;
import com.dataloom.authorization.Authorization;
import com.dataloom.authorization.Permission;
import com.dataloom.datastore.authentication.AuthenticationTestBase;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

//Awkward test naming, to force JUnit Test runs in correct order
@FixMethodOrder( MethodSorters.NAME_ASCENDING )
public class RequestsControllerTest extends AuthenticationTestBase {
    protected static EntityType          et;
    protected static EntitySet           es;

    protected static List<UUID>          entitySetAclKey;
    protected static Set<List<UUID>>     propertiesAclKeys;

    protected static EnumSet<Permission> entitySetPermissions  = EnumSet.of( Permission.READ, Permission.WRITE );
    protected static EnumSet<Permission> propertiesPermissions = EnumSet.of( Permission.READ );

    protected static int                 entitySetRequestMade  = 0;
    protected static int                 propertiesRequestMade = 0;
    protected static int                 totalRequestMade      = 0;

    @BeforeClass
    public static void init() {
        loginAs( "support" );
        createEntitySet();
        updateAclKeys();
    }

    @Test
    public void test1RequestPermissions() {
        loginAs( "dummyuser" );

        Set<Request> req = new HashSet<>();

        // Request Entity Set
        req.add( new Request( entitySetAclKey, entitySetPermissions ) );
        entitySetRequestMade++;

        // Request Properties
        propertiesAclKeys
                .forEach( aclKey -> {
                    req.add( new Request( aclKey, propertiesPermissions ) );
                    propertiesRequestMade++;
                } );

        requestsApi.submit( req );
        totalRequestMade = entitySetRequestMade + propertiesRequestMade;
    }

    @Test
    public void test2CheckSubmittedRequests() {
        // Check user submitted requests
        loginAs( "dummyuser" );
        Assert.assertEquals( totalRequestMade, Iterables.size( requestsApi.getMyRequests() ) );
        Assert.assertEquals( totalRequestMade, Iterables.size( requestsApi.getMyRequests( RequestStatus.SUBMITTED ) ) );
        Assert.assertEquals( 0, Iterables.size( requestsApi.getMyRequests( RequestStatus.APPROVED ) ) );
        
        Assert.assertEquals( totalRequestMade,
                Iterables.size( requestsApi
                        .getStatuses( Sets.union( ImmutableSet.of( entitySetAclKey ), propertiesAclKeys ) ) ) );
        Assert.assertEquals( totalRequestMade,
                Iterables.size( requestsApi.getStatuses( RequestStatus.SUBMITTED,
                        Sets.union( ImmutableSet.of( entitySetAclKey ), propertiesAclKeys ) ) ) );
        Assert.assertEquals( 0,
                Iterables.size( requestsApi.getStatuses( RequestStatus.APPROVED,
                        Sets.union( ImmutableSet.of( entitySetAclKey ), propertiesAclKeys ) ) ) );

        // Check owner received requests
        loginAs( "support" );
        Assert.assertEquals( totalRequestMade,
                Iterables.size( requestsApi
                        .getStatuses( Sets.union( ImmutableSet.of( entitySetAclKey ), propertiesAclKeys ) ) ) );
        Assert.assertEquals( totalRequestMade,
                Iterables.size( requestsApi.getStatuses( RequestStatus.SUBMITTED,
                        Sets.union( ImmutableSet.of( entitySetAclKey ), propertiesAclKeys ) ) ) );
        Assert.assertEquals( 0,
                Iterables.size( requestsApi.getStatuses( RequestStatus.APPROVED,
                        Sets.union( ImmutableSet.of( entitySetAclKey ), propertiesAclKeys ) ) ) );
    }

    @Test
    public void test3ApproveRequests() {
        loginAs( "support" );
        Set<Status> approvedSet = StreamSupport.stream( requestsApi.getStatuses( RequestStatus.SUBMITTED,
                Sets.union( ImmutableSet.of( entitySetAclKey ), propertiesAclKeys ) ).spliterator(), false )
                .map( status -> {
                    // Approve each request
                    Status approved = new Status(
                            status.getAclKey(),
                            status.getPrincipal(),
                            status.getPermissions(),
                            RequestStatus.APPROVED );
                    return approved;
                } )
                .collect( Collectors.toSet() );

        requestsApi.updateStatuses( approvedSet );

        // Check owner received requests
        Assert.assertEquals( totalRequestMade,
                Iterables.size( requestsApi.getStatuses( RequestStatus.APPROVED,
                        Sets.union( ImmutableSet.of( entitySetAclKey ), propertiesAclKeys ) ) ) );
        Assert.assertEquals( 0,
                Iterables.size( requestsApi.getStatuses( RequestStatus.SUBMITTED,
                        Sets.union( ImmutableSet.of( entitySetAclKey ), propertiesAclKeys ) ) ) );

    }

    @Test
    public void test4CheckPermissions() {
        loginAs( "dummyuser" );
        Assert.assertEquals( totalRequestMade, Iterables.size( requestsApi.getMyRequests() ) );
        Assert.assertEquals( 0, Iterables.size( requestsApi.getMyRequests( RequestStatus.SUBMITTED ) ) );
        Assert.assertEquals( totalRequestMade, Iterables.size( requestsApi.getMyRequests( RequestStatus.APPROVED ) ) );

        // Verify permissions via authorizations api
        Iterable<Authorization> entitySetAuth = authorizationsApi
                .checkAuthorizations( ImmutableSet.of( new AccessCheck( entitySetAclKey, EnumSet.allOf( Permission.class ) ) ) );
        Assert.assertEquals( entitySetRequestMade, Iterables.size( entitySetAuth ) );

        authorizationsApi
        .checkAuthorizations( ImmutableSet.of( new AccessCheck( entitySetAclKey, EnumSet.allOf( Permission.class ) ) ) )
        .forEach( auth -> checkPermissionsMap( auth.getPermissions(), entitySetPermissions ) );

        // Verify permissions via authorizations api
        Iterable<Authorization> propertiesAuth = authorizationsApi.checkAuthorizations(
                propertiesAclKeys.stream().map( aclKey -> new AccessCheck( aclKey, EnumSet.allOf( Permission.class ) ) )
                        .collect( Collectors.toSet() ) );
        Assert.assertEquals( propertiesRequestMade, Iterables.size( propertiesAuth ) );
        
        authorizationsApi.checkAuthorizations(
                propertiesAclKeys.stream().map( aclKey -> new AccessCheck( aclKey, EnumSet.allOf( Permission.class ) ) )
                        .collect( Collectors.toSet() ) )
        .forEach( auth -> checkPermissionsMap( auth.getPermissions(), propertiesPermissions ) );
    }

    /**
     * Auxiliary functions for the test
     */

    public static void updateAclKeys() {
        entitySetAclKey = ImmutableList.of( es.getId() );

        propertiesAclKeys = et.getProperties().stream()
                .map( ptId -> ImmutableList.of( es.getId(), ptId ) )
                .collect( Collectors.toSet() );
    }

    private static void checkPermissionsMap(
            Map<Permission, Boolean> permissionMap,
            EnumSet<Permission> expectedPermissions ) {
        EnumSet.allOf( Permission.class ).forEach( permission -> {
            Assert.assertEquals( expectedPermissions.contains( permission ), permissionMap.get( permission ) );
        } );
    }

    /**
     * Utility functions for initializing entity sets
     */

    public static PropertyType createPropertyType() {
        PropertyType pt = TestDataFactory.propertyType();
        UUID propertyTypeId = edmApi.createPropertyType( pt );

        Assert.assertNotNull( "Property type creation returned null value.", propertyTypeId );

        return pt;
    }

    public static EntityType createEntityType() {
        PropertyType p1 = createPropertyType();
        PropertyType k = createPropertyType();
        PropertyType p2 = createPropertyType();

        EntityType expected = TestDataFactory.entityType( k );
        expected.removePropertyTypes( expected.getProperties() );
        expected.addPropertyTypes( ImmutableSet.of( k.getId(), p1.getId(), p2.getId() ) );
        UUID entityTypeId = edmApi.createEntityType( expected );
        Assert.assertNotNull( "Entity type creation shouldn't return null UUID.", entityTypeId );

        et = expected;

        return expected;
    }

    public static void createEntitySet() {
        EntityType entityType = createEntityType();

        EntitySet newES = new EntitySet(
                UUID.randomUUID(),
                entityType.getId(),
                RandomStringUtils.randomAlphanumeric( 10 ),
                "foobar",
                Optional.<String> of( "barred" ) );

        Map<String, UUID> entitySetIds = edmApi.createEntitySets( ImmutableSet.of( newES ) );
        //debug by Ho Chung
        Assert.assertTrue( "Entity Set creation does not return correct UUID",
                entitySetIds.values().contains( newES.getId() ) );

        es = newES;
    }
}
