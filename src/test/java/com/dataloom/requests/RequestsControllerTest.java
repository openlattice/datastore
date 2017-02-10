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

package com.dataloom.requests;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.dataloom.authorization.AccessCheck;
import com.dataloom.authorization.Authorization;
import com.dataloom.authorization.Permission;
import com.dataloom.datastore.authentication.MultipleAuthenticatedUsersBase;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

//Awkward test naming, to force JUnit Test runs in correct order
@FixMethodOrder( MethodSorters.NAME_ASCENDING )
public class RequestsControllerTest extends MultipleAuthenticatedUsersBase {
    protected static EntityType          et;
    protected static EntitySet           es;

    protected static List<UUID>          entitySetAclKey;
    protected static Set<List<UUID>>     propertiesAclKeys;
    protected static Set<List<UUID>>     allAclKeys;

    protected static EnumSet<Permission> entitySetPermissions  = EnumSet.of( Permission.READ, Permission.WRITE );
    protected static EnumSet<Permission> propertiesPermissions = EnumSet.of( Permission.READ );

    protected static int                 entitySetRequestMade  = 0;
    protected static int                 propertiesRequestMade = 0;
    protected static int                 totalRequestMade      = 0;

    @BeforeClass
    public static void init() {
        loginAs( "admin" );
        et = createEntityType();
        es = createEntitySet( et );

        entitySetAclKey = ImmutableList.of( es.getId() );
        propertiesAclKeys = et.getProperties().stream()
                .map( ptId -> ImmutableList.of( es.getId(), ptId ) )
                .collect( Collectors.toSet() );
        allAclKeys = Sets.union( ImmutableSet.of( entitySetAclKey ), propertiesAclKeys );
    }

    @Test
    public void test1RequestPermissions() {
        loginAs( "user1" );
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
        loginAs( "user1" );
        Assert.assertEquals( totalRequestMade, Iterables.size( requestsApi.getMyRequests() ) );
        Assert.assertEquals( totalRequestMade, Iterables.size( requestsApi.getMyRequests( RequestStatus.SUBMITTED ) ) );
        Assert.assertEquals( 0, Iterables.size( requestsApi.getMyRequests( RequestStatus.APPROVED ) ) );

        Assert.assertEquals( totalRequestMade,
                Iterables.size( requestsApi
                        .getStatuses( allAclKeys ) ) );
        Assert.assertEquals( totalRequestMade,
                Iterables.size( requestsApi.getStatuses( RequestStatus.SUBMITTED,
                        allAclKeys ) ) );
        Assert.assertEquals( 0,
                Iterables.size( requestsApi.getStatuses( RequestStatus.APPROVED,
                        allAclKeys ) ) );

        // Check owner received requests
        loginAs( "admin" );
        Assert.assertEquals( totalRequestMade,
                Iterables.size( requestsApi
                        .getStatuses( allAclKeys ) ) );
        Assert.assertEquals( totalRequestMade,
                Iterables.size( requestsApi.getStatuses( RequestStatus.SUBMITTED,
                        allAclKeys ) ) );
        Assert.assertEquals( 0,
                Iterables.size( requestsApi.getStatuses( RequestStatus.APPROVED,
                        allAclKeys ) ) );
    }

    @Test
    public void test3ApproveRequests() {
        loginAs( "admin" );
        Set<Status> approvedSet = StreamSupport.stream( requestsApi.getStatuses( RequestStatus.SUBMITTED,
                allAclKeys ).spliterator(), false )
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
                        allAclKeys ) ) );
        Assert.assertEquals( 0,
                Iterables.size( requestsApi.getStatuses( RequestStatus.SUBMITTED,
                        allAclKeys ) ) );

    }

    @Test
    public void test4CheckPermissions() {
        loginAs( "user1" );
        Assert.assertEquals( totalRequestMade, Iterables.size( requestsApi.getMyRequests() ) );
        Assert.assertEquals( 0, Iterables.size( requestsApi.getMyRequests( RequestStatus.SUBMITTED ) ) );
        Assert.assertEquals( totalRequestMade, Iterables.size( requestsApi.getMyRequests( RequestStatus.APPROVED ) ) );

        // Verify permissions via authorizations api
        Iterable<Authorization> entitySetAuth = authorizationsApi
                .checkAuthorizations(
                        ImmutableSet.of( new AccessCheck( entitySetAclKey, EnumSet.allOf( Permission.class ) ) ) );
        Assert.assertEquals( entitySetRequestMade, Iterables.size( entitySetAuth ) );

        checkUserPermissions( entitySetAclKey, entitySetPermissions );

        // Verify permissions via authorizations api
        Iterable<Authorization> propertiesAuth = authorizationsApi.checkAuthorizations(
                propertiesAclKeys.stream().map( aclKey -> new AccessCheck( aclKey, EnumSet.allOf( Permission.class ) ) )
                        .collect( Collectors.toSet() ) );
        Assert.assertEquals( propertiesRequestMade, Iterables.size( propertiesAuth ) );

        propertiesAclKeys.stream().forEach( aclKey -> checkUserPermissions( aclKey, propertiesPermissions ) );
    }
}
