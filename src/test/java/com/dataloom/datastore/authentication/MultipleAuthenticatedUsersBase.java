package com.dataloom.datastore.authentication;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;

import com.auth0.jwt.internal.org.apache.commons.lang3.RandomStringUtils;
import com.dataloom.authorization.AccessCheck;
import com.dataloom.authorization.AuthorizationsApi;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.PermissionsApi;
import com.dataloom.data.DataApi;
import com.dataloom.datastore.BootstrapDatastoreWithCassandra;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.requests.RequestsApi;
import com.dataloom.search.SearchApi;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import retrofit2.Retrofit;

public class MultipleAuthenticatedUsersBase extends BootstrapDatastoreWithCassandra {
    protected static Map<String, Retrofit> retrofitMap = new HashMap<>();
    
    protected static EdmApi edmApi;
    protected static PermissionsApi permissionsApi;
    protected static AuthorizationsApi authorizationsApi;
    protected static RequestsApi requestsApi;
    protected static DataApi dataApi;
    protected static SearchApi searchApi;

    static{
        retrofitMap.put( "admin", retrofit );
        retrofitMap.put( "user1", retrofit1 );
        retrofitMap.put( "user2", retrofit2 );
        retrofitMap.put( "user3", retrofit3 );
    }

    /**
     * Auxiliary functions
     */

    public static void loginAs( String user ){
        //update Api instances involved
        Retrofit currentRetrofit = retrofitMap.get( user );
        if( currentRetrofit == null ){
            throw new IllegalArgumentException( "User does not exist in Retrofit map." );
        }
        edmApi = currentRetrofit.create( EdmApi.class );
        permissionsApi = currentRetrofit.create( PermissionsApi.class );
        authorizationsApi = currentRetrofit.create( AuthorizationsApi.class );
        requestsApi = currentRetrofit.create( RequestsApi.class );
        dataApi = currentRetrofit.create( DataApi.class );
        searchApi = currentRetrofit.create( SearchApi.class );
    }
    
    /**
     * Helper methods for AuthorizationsApi
     */
    
    public static void checkPermissionsMap(
            Map<Permission, Boolean> permissionMap,
            EnumSet<Permission> expectedPermissions ) {
        EnumSet.allOf( Permission.class ).forEach( permission -> {
            Assert.assertEquals( expectedPermissions.contains( permission ), permissionMap.get( permission ) );
        } );
    }
    
    public static void checkUserPermissions( List<UUID> aclKey, EnumSet<Permission> expected ){
        authorizationsApi
        .checkAuthorizations( ImmutableSet.of( new AccessCheck( aclKey, EnumSet.allOf( Permission.class ) ) ) )
        .forEach( auth -> checkPermissionsMap( auth.getPermissions(), expected ) );   
    }

    /**
     * Helper methods for EdmApi
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

        return expected;
    }

    public static EntitySet createEntitySet() {
        EntityType entityType = createEntityType();

        return createEntitySet( entityType );
    }

    
    public static EntitySet createEntitySet( EntityType entityType ){
        EntitySet newES = new EntitySet(
                UUID.randomUUID(),
                entityType.getId(),
                RandomStringUtils.randomAlphanumeric( 10 ),
                "foobar",
                Optional.<String> of( "barred" ) );

        Map<String, UUID> entitySetIds = edmApi.createEntitySets( ImmutableSet.of( newES ) );

        Assert.assertTrue( "Entity Set creation does not return correct UUID",
                entitySetIds.values().contains( newES.getId() ) );

        return newES;

    }
}
