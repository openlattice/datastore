package com.dataloom.datastore.permissions;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.spark_project.guava.collect.Iterables;

import com.auth0.jwt.internal.org.apache.commons.lang3.RandomStringUtils;
import com.dataloom.authorization.AccessCheck;
import com.dataloom.authorization.Ace;
import com.dataloom.authorization.Acl;
import com.dataloom.authorization.AclData;
import com.dataloom.authorization.Action;
import com.dataloom.authorization.AuthorizationsApi;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.PermissionsApi;
import com.dataloom.datastore.BootstrapDatastoreWithCassandra;
import com.dataloom.datastore.authentication.MultipleAuthenticatedUsersBase;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import retrofit2.Retrofit;

public class PermissionsControllerTest extends MultipleAuthenticatedUsersBase {
    protected static List<UUID> entitySetAclKey;

    @BeforeClass
    public static void init(){
        loginAs( "admin" );
        EntitySet es = createEntitySet();
        entitySetAclKey = ImmutableList.of( es.getId() );
    }
    
    @Test
    public void testAddPermission(){
        //sanity check: user1 has no permission of the entity set
        loginAs( "user1" );
        checkUserPermissions( entitySetAclKey, EnumSet.noneOf( Permission.class ));
        
        //add Permissions
        loginAs( "admin" );
        EnumSet<Permission> newPermissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Acl acl = new Acl( entitySetAclKey, ImmutableSet.of( new Ace( user1, newPermissions ) ));
        
        permissionsApi.updateAcl( new AclData( acl, Action.ADD) );
        
        //Check: user1 now has correct permissions of the entity set
        loginAs( "user1" );
        checkUserPermissions( entitySetAclKey, newPermissions );     
    }
    
    @Test
    public void testSetPermission(){
        //Setup: add Permissions
        loginAs( "admin" );
        EnumSet<Permission> oldPermissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Acl oldAcl = new Acl( entitySetAclKey, ImmutableSet.of( new Ace( user2, oldPermissions ) ));        
        permissionsApi.updateAcl( new AclData( oldAcl, Action.ADD) );
        //sanity check: user2 has oldPermissions on entity set
        loginAs( "user2" );
        checkUserPermissions( entitySetAclKey, oldPermissions );

        //set Permissions
        loginAs( "admin" );
        EnumSet<Permission> newPermissions = EnumSet.of( Permission.DISCOVER, Permission.WRITE );
        Acl newAcl = new Acl( entitySetAclKey, ImmutableSet.of( new Ace( user2, newPermissions ) ));        
        permissionsApi.updateAcl( new AclData( newAcl, Action.SET) );

        //Check: user2 now has new permissions of the entity set
        loginAs( "user2" );
        checkUserPermissions( entitySetAclKey, newPermissions );         
    }
    
    @Test
    public void testRemovePermission(){
        //Setup: add Permissions
        loginAs( "admin" );
        EnumSet<Permission> oldPermissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Acl oldAcl = new Acl( entitySetAclKey, ImmutableSet.of( new Ace( user3, oldPermissions ) ));        
        permissionsApi.updateAcl( new AclData( oldAcl, Action.ADD) );
        //sanity check: user3 has oldPermissions on entity set
        loginAs( "user3" );
        checkUserPermissions( entitySetAclKey, oldPermissions );

        //remove Permissions
        loginAs( "admin" );
        EnumSet<Permission> remove = EnumSet.of( Permission.READ );
        EnumSet<Permission> newPermissions = EnumSet.of( Permission.DISCOVER );
        Acl newAcl = new Acl( entitySetAclKey, ImmutableSet.of( new Ace( user3, remove ) ));        
        permissionsApi.updateAcl( new AclData( newAcl, Action.REMOVE) );

        //Check: user3 now has new permissions of the entity set
        loginAs( "user3" );
        checkUserPermissions( entitySetAclKey, newPermissions );         
    }
    
    @Test
    public void testGetAcl(){
        loginAs( "admin" );
        EntitySet es = createEntitySet();
        List<UUID> aclKey = ImmutableList.of( es.getId() );
        
        //sanity check: only admin has permissions
        Assert.assertEquals( 1, Iterables.size( permissionsApi.getAcl( aclKey ).getAces() ) );
        
        //give user1 permissions;
        EnumSet<Permission> permissions = EnumSet.of( Permission.DISCOVER, Permission.READ );
        Ace ace = new Ace( user1, permissions );
        Acl acl = new Acl( aclKey, ImmutableSet.of( ace ));
        permissionsApi.updateAcl( new AclData( acl, Action.ADD) );

        //Check: getAcl should return user1's permissions info, i.e. contains user1's ace
        Acl result = permissionsApi.getAcl( aclKey );
        Assert.assertTrue( Iterables.contains( result.getAces(), ace) );
    }
}
