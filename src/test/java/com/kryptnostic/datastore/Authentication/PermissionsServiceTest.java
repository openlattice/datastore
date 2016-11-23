package com.kryptnostic.datastore.Authentication;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.NotFoundException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.auth0.Auth0;
import com.auth0.authentication.AuthenticationAPIClient;
import com.dataloom.authorization.requests.Action;
import com.dataloom.authorization.requests.EntitySetAclRequest;
import com.dataloom.authorization.requests.EntityTypeAclRequest;
import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.PermissionsInfo;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.authorization.requests.PrincipalType;
import com.dataloom.authorization.requests.PropertyTypeInEntityTypeAclRequest;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.data.DataApi;
import com.dataloom.data.requests.CreateEntityRequest;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntitySetWithPermissions;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.requests.PropertyTypeInEntitySetAclRequest;
import com.dataloom.permissions.PermissionsApi;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.kryptnostic.datastore.Datastore;
import com.kryptnostic.datastore.services.DataService;
import com.kryptnostic.datastore.services.EdmService;
import com.kryptnostic.datastore.services.PermissionsService;
import com.squareup.okhttp.OkHttpClient;

import digital.loom.rhizome.authentication.AuthenticationTest;
import digital.loom.rhizome.configuration.auth0.Auth0Configuration;
import retrofit.client.OkClient;
import retrofit2.Retrofit;

public class PermissionsServiceTest {

    protected static final Principal         ROLE_USER             = new Principal( PrincipalType.ROLE, "user" );
    protected static final Principal         USER_USER             = new Principal(
            PrincipalType.USER,
            "support@kryptnostic.com" );

    protected static final String            NATION_NAMESPACE      = "us";
    protected static final FullQualifiedName NATION_SCHEMA         = new FullQualifiedName(
            NATION_NAMESPACE,
            "schema" );                                                                                          // schema

    protected static final FullQualifiedName NATION_CITIZENS       = new FullQualifiedName(
            NATION_NAMESPACE,
            "citizens" );                                                                                        // entity
                                                                                                                 // type
    protected static final String            NATION_SECRET_SERVICE = "secret_service";                           // entity
                                                                                                                 // set
                                                                                                                 // name

    protected static final FullQualifiedName EMPLOYEE_ID           = new FullQualifiedName(
            NATION_NAMESPACE,
            "employee_id" );

    protected static final FullQualifiedName LIFE_EXPECTANCY       = new FullQualifiedName(
            NATION_NAMESPACE,
            "life_expectancy" );                                                                                 // property
                                                                                                                 // type
    protected static final FullQualifiedName ADDRESS               = new FullQualifiedName(
            NATION_NAMESPACE,
            "address" );                                                                                         // property
                                                                                                                 // type
    protected static final FullQualifiedName POSITION              = new FullQualifiedName(
            NATION_NAMESPACE,
            "position" );                                                                                        // property
                                                                                                                 // type

    protected static final FullQualifiedName SPIED_ON              = new FullQualifiedName(
            NATION_NAMESPACE,
            "spied_on" );                                                                                        // property
                                                                                                                 // type

    private static final Logger              logger                = LoggerFactory.getLogger( Auth0Test.class );
    protected static final Datastore         ds                    = new Datastore();
    protected static Auth0Configuration      configuration;
    protected static Auth0                   auth0;
    protected static AuthenticationAPIClient client;
    protected static DataApi                 dataApi;
    protected static EdmApi                  edmApi;
    protected static EdmService              edmService;
    protected static PermissionsApi          ps;
    protected static PermissionsService      permissionsService;

    protected static Retrofit                dataServiceRestAdapter;

    @BeforeClass
    public static void init() throws Exception {
        ds.start( "local", "cassandra" );
        configuration = ds.getContext().getBean( Auth0Configuration.class );
        auth0 = new Auth0( configuration.getClientId(), configuration.getDomain() );
        client = auth0.newAuthenticationAPIClient();
        String jwtToken = AuthenticationTest.authenticate().getLeft().getIdToken();
        OkHttpClient httpClient = new OkHttpClient();
        httpClient.setConnectTimeout( 60, TimeUnit.SECONDS );
        httpClient.setReadTimeout( 60, TimeUnit.SECONDS );
        OkClient okClient = new OkClient( httpClient );
        dataServiceRestAdapter = RetrofitFactory.newClient( Environment.TESTING, () -> jwtToken );
        dataApi = dataServiceRestAdapter.create( DataApi.class );
        edmApi = dataServiceRestAdapter.create( EdmApi.class );
        ps = dataServiceRestAdapter.create( PermissionsApi.class );
        edmService = ds.getContext().getBean( EdmService.class );
        permissionsService = ds.getContext().getBean( PermissionsService.class );
    }

    @Test
    public void permissionsServiceTest() {
        createTypes();

        System.err.println( "*********************" );
        System.err.println( "ROLE TESTS START!" );
        System.err.println( "*********************" );

        entityTypeTest( ROLE_USER );
        entitySetTest( ROLE_USER );
        propertyTypeInEntityTypeTest( ROLE_USER );
        propertyTypeInEntitySetTest( ROLE_USER );

        System.err.println( "*********************" );
        System.err.println( "ROLE TESTS END!" );
        System.err.println( "*********************" );

        System.err.println( "*********************" );
        System.err.println( "USER TESTS START!" );
        System.err.println( "*********************" );

        entityTypeTest( USER_USER );
        entitySetTest( USER_USER );
        propertyTypeInEntityTypeTest( USER_USER );
        propertyTypeInEntitySetTest( USER_USER );

        System.err.println( "*********************" );
        System.err.println( "USER TESTS END!" );
        System.err.println( "*********************" );

        System.err.println( "*********************" );
        System.err.println( "REQUEST ACCESS TESTS START!" );
        System.err.println( "*********************" );

        requestAccess();

        System.err.println( "*********************" );
        System.err.println( "REQUEST ACCESS TESTS END!" );
        System.err.println( "*********************" );
    }

    @AfterClass 
    public static void cleanUp() {
        // Give permissions
        ps.updateEntityTypesAcls(
                ImmutableSet.of( new EntityTypeAclRequest().setPrincipal( ROLE_USER ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPermissions( EnumSet.of( Permission.ALTER ) ) ) );

        // Delete
        edmApi.deletePropertyType( EMPLOYEE_ID.getNamespace(), EMPLOYEE_ID.getName() );
        edmApi.deletePropertyType( LIFE_EXPECTANCY.getNamespace(), LIFE_EXPECTANCY.getName() );
        edmApi.deletePropertyType( ADDRESS.getNamespace(), ADDRESS.getName() );
        edmApi.deletePropertyType( POSITION.getNamespace(), POSITION.getName() );

        edmApi.deleteEntityType( NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName() );

        System.err.println( "*****And the nation fell out of sight..*****" );
        System.err.println( "*****Test ends!*****" );
    }

    private void createTypes() {
        // Create property types Employee-id, Address, Position, Life expectancy
        PropertyType employeeId = new PropertyType().setNamespace( NATION_NAMESPACE )
                .setName( EMPLOYEE_ID.getName() )
                .setDatatype( EdmPrimitiveTypeKind.Guid ).setMultiplicity( 0 );
        PropertyType lifeExpectancy = new PropertyType().setNamespace( NATION_NAMESPACE )
                .setName( LIFE_EXPECTANCY.getName() )
                .setDatatype( EdmPrimitiveTypeKind.Int32 ).setMultiplicity( 0 );
        PropertyType address = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( ADDRESS.getName() )
                .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 );
        PropertyType position = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( POSITION.getName() )
                .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 );
        edmApi.createPropertyType( employeeId );
        edmApi.createPropertyType( lifeExpectancy );
        edmApi.createPropertyType( address );
        edmApi.createPropertyType( position );

        // creates entity type Citizen
        EntityType citizens = new EntityType().setNamespace( NATION_NAMESPACE ).setName( NATION_CITIZENS.getName() )
                .setKey( ImmutableSet.of( EMPLOYEE_ID ) )
                .setProperties( ImmutableSet.of(
                        EMPLOYEE_ID,
                        LIFE_EXPECTANCY,
                        ADDRESS,
                        POSITION ) );
        edmApi.postEntityType( citizens );

        try{
            // God creates entity set Secret Service
            EntitySet secretService = new EntitySet().setType( NATION_CITIZENS )
                    .setName( NATION_SECRET_SERVICE )
                    .setTitle( "Every nation would have one" );
            edmService.createEntitySet( secretService );
        } catch ( IllegalArgumentException e ){
            // This would happen if entity set already exists
            System.err.println( e );
        }
    }

    private EntityType entityTypeMetadataLookup( FullQualifiedName entityTypeFqn ) {
        EntityType result = edmApi.getEntityType( entityTypeFqn.getNamespace(), entityTypeFqn.getName() );
        System.err.println( "Getting Entity Types metadata for " + entityTypeFqn + ": " + result );
        return result;
    }

    private void entityTypeTest( Principal principal ) {
        System.err.println( "***Entity Type Test starts***" );
        // Get metadata for NATION_CITIZENS
        entityTypeMetadataLookup( NATION_CITIZENS );

        System.err.println( "Test 1 Starts!" );
        // Test 1: Citizen has ALTER permission for NATION_CITIZENS
        // Expected: RandomGuy can add/remove property types from NATION_CITIZENS.

        ps.updateEntityTypesAcls(
                ImmutableSet.of( new EntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPermissions( EnumSet.of( Permission.ALTER ) ) ) );

        // Current setting is everyone can create types
        PropertyType spiedOn = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( SPIED_ON.getName() )
                .setDatatype( EdmPrimitiveTypeKind.Boolean ).setMultiplicity( 0 );
        edmApi.createPropertyType( spiedOn );

        edmApi.addPropertyTypesToEntityType( NATION_CITIZENS.getNamespace(),
                NATION_CITIZENS.getName(),
                ImmutableSet.of( SPIED_ON ) );

        System.err.println( "Expected: Property SPIED_ON is added to entity Type" );
        entityTypeMetadataLookup( NATION_CITIZENS );

        System.err.println( "Expected: Property SPIED_ON is removed from entity Type" );
        edmApi.removePropertyTypesFromEntityType( NATION_CITIZENS.getNamespace(),
                NATION_CITIZENS.getName(),
                ImmutableSet.of( SPIED_ON ) );
        entityTypeMetadataLookup( NATION_CITIZENS );

        ps.updateEntityTypesAcls(
                ImmutableSet.of( new EntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPermissions( EnumSet.of( Permission.ALTER ) ) ) );

        System.err.println( "Test 2 Starts!" );
        // Test 2: Citizen has ALTER permission for NATION_CITIZENS
        // Expected: RandomGuy can delete the entity type NATION_CITIZENS. (Which also means the entity sets inside are
        // deleted)
        ps.updateEntityTypesAcls(
                ImmutableSet.of( new EntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPermissions( EnumSet.of( Permission.ALTER ) ) ) );

        edmApi.deleteEntityType( NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName() );
        System.err.println( "Expected: Entity Type NATION_CITIZENS is removed." );
        System.err.println( "Print all entity types:" );
        for ( EntityType entityType : edmApi.getEntityTypes() ) {
            System.err.println( entityType );
        }
        System.err.println( "Printing finished." );

        entityTypeTestCleanup();
    }

    private void entityTypeTestCleanup() {
        System.err.println( " *** Entity Type Test Clean Up Happening *** " );
        // Remove property type SPIED_ON
        edmApi.deletePropertyType( SPIED_ON.getNamespace(), SPIED_ON.getName() );

        // create the entity type and entity set back
        EntityType citizens = new EntityType().setNamespace( NATION_NAMESPACE ).setName( NATION_CITIZENS.getName() )
                .setKey( ImmutableSet.of( EMPLOYEE_ID ) )
                .setProperties( ImmutableSet.of( EMPLOYEE_ID,
                        LIFE_EXPECTANCY,
                        ADDRESS,
                        POSITION ) );
        edmApi.postEntityType( citizens );

        // God creates entity set Secret Service
        EntitySet secretService = new EntitySet().setType( NATION_CITIZENS )
                .setName( NATION_SECRET_SERVICE )
                .setTitle( "Every nation would have one" );
        edmService.createEntitySet( secretService );

        System.err.println( " *** Entity Type Test Clean Up Finished *** " );
    }

    private void entitySetTest( Principal principal ) {
        System.err.println( "***Entity Set Test starts***" );

        System.err.println( "--- Test 1 ---" );
        // Test 1: Citizen is given the DISCOVER permission for Secret Service; and the right got removed after.
        // Expected: RandomGuy can see the metadata for Secret Service; and cannot after.
        ps.updateEntitySetsAcls(
                ImmutableSet.of( new EntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPermissions( EnumSet.of( Permission.DISCOVER ) ) ) );
        Assert.assertNotEquals( 0, Iterables.size( edmApi.getEntitySets( null ) ) );

        ps.updateEntitySetsAcls(
                ImmutableSet.of( new EntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPermissions( EnumSet.of( Permission.DISCOVER ) ) ) );
        Assert.assertEquals( 0, Iterables.size( edmApi.getEntitySets( null ) ) );

        // Setup: Citizen creates a new entity set in NATION_CITIZENS.
        String DYSTOPIANS = "dystopians";

        EntitySet dystopians = new EntitySet().setType( NATION_CITIZENS )
                .setName( DYSTOPIANS )
                .setTitle( "We could be in one now" );
        edmApi.postEntitySets( ImmutableSet.of( dystopians ) );

        // Test 2: Check Citizen's permissions for the entity set.
        // Expected: Citizen has all permissions for the entity set.
        System.err.println( "--- Test 2 ---" );
        System.err.println( "Permissions for " + DYSTOPIANS + " is " + ps.getEntitySetAclsForUser( DYSTOPIANS ) );

        // Test 3: Given Citizen DISCOVER permission for Secret Service. Citizen does getEntitySets.
        // Expected: Citizen discovers SECRET_SERVICE, has owner permissions for new entity set.
        ps.updateEntitySetsAcls(
                ImmutableSet.of( new EntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPermissions( EnumSet.of( Permission.DISCOVER ) ) ) );
        System.err.println( "--- Test 3 ---" );
        System.err.println( "Permissions for entity sets are:" );
        for ( EntitySetWithPermissions entitySetWithPermissions : edmApi.getEntitySets( null ) ) {
            System.err.println( "Entity Set: " + entitySetWithPermissions.getEntitySet().getName() + " Permissions "
                    + entitySetWithPermissions.getPermissions() );
        }
        // Test 3.5: Remove DISCOVER permission for Secret Service. Citizen does getEntitySets
        // Expected: Citizen does not discover SECRET_SERVICE, has owner permissions for new entity set.
        ps.updateEntitySetsAcls(
                ImmutableSet.of( new EntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPermissions( EnumSet.of( Permission.DISCOVER ) ) ) );
        System.err.println( "--- Test 3.5 ---" );
        System.err.println( "Permissions for entity sets are:" );
        for ( EntitySetWithPermissions entitySet : edmApi.getEntitySets( null ) ) {
            System.err.println( "Entity Set: " + entitySet.getEntitySet().getName() + " Permissions "
                    + entitySet.getPermissions() );
        }

        // Test 4: Give permissions of the entity set to users and roles. Citizens check for permissions for the entity
        // set
        // Expected: Listing all permissions given for the entity set
        ps.updateEntitySetsAcls(
                ImmutableSet.of( new EntitySetAclRequest()
                        .setPrincipal( new Principal( PrincipalType.ROLE, "ROLE_DISCOVER" ) ).setAction( Action.ADD )
                        .setName( DYSTOPIANS ).setPermissions( EnumSet.of( Permission.DISCOVER ) ),
                        new EntitySetAclRequest().setPrincipal( new Principal( PrincipalType.ROLE, "ROLE_READWRITE" ) )
                                .setAction( Action.ADD )
                                .setName( DYSTOPIANS )
                                .setPermissions( EnumSet.of( Permission.READ, Permission.WRITE ) ),
                        new EntitySetAclRequest().setPrincipal( new Principal( PrincipalType.USER, "USER_EVERYTHING" ) )
                                .setAction( Action.ADD )
                                .setName( DYSTOPIANS ).setPermissions(
                                        EnumSet.of( Permission.DISCOVER, Permission.READ, Permission.WRITE ) ) ) );
        System.err.println( "--- Test 4 ---" );
        System.err.println( "All permissions for the entity set " + DYSTOPIANS + ":" );
        for ( PermissionsInfo info : ps.getEntitySetAclsForOwner( DYSTOPIANS ) ) {
            System.err.println( info.getPrincipal() + " has Permissions " + info.getPermissions() );
        }
    }

    /**
     * WARNING: This test creates garbage data. WARNING: Check tables to see if output is correct.
     */
    private void propertyTypeInEntityTypeTest( Principal principal ) {
        System.err.println( "***Property Type In Entity Type Test Starts***" );
        // Give user READ rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in NATION_CITIZENS, as well as
        // READ rights for NATION_CITIZENS
        // Give user WRITE rights for NATION_CITIZENS
        ps.updateEntityTypesAcls( ImmutableSet.of(
                new EntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD ).setType( NATION_CITIZENS )
                        .setPermissions( EnumSet.of( Permission.READ, Permission.WRITE ) ) ) );
        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.READ ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.READ ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( POSITION )
                        .setPermissions( EnumSet.of( Permission.READ ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( EnumSet.of( Permission.READ ) ) ) );

        // Test 1: give Writer WRITE rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in NATION_CITIZENS.
        // Expected: Being able to read and write all data types.

        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( POSITION )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ) ) );

        // Test 1.33: Actually write data to entity type
        createData( 10,
                NATION_CITIZENS,
                Optional.absent(),
                ImmutableSet.of( EMPLOYEE_ID,
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 1.67: Read data from entity type - should be able to read all data.
        System.err.println( " -- READ TEST 1 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result1 = dataApi.getAllEntitiesOfType( NATION_CITIZENS );
        for ( Multimap<FullQualifiedName, Object> entity : result1 ) {
            System.err.println( entity );
        }

        // Cleanup: remove WRITE rights for User.
        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( POSITION )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ) ) );

        // Test 2: given User WRITE rights for EMPLOYEE_ID, ADDRESS in NATION_CITIZENS
        // Expected: Only have WRITE rights for (EMPLOYEE_ID, NATION_CITIZENS), (ADDRESS, NATION_CITIZENS)

        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ) ) );

        // Test 2.33: Actually write data - only the columns EMPLOYEE_ID, ADDRESS would be non-empty
        createData( 10,
                NATION_CITIZENS,
                Optional.absent(),
                ImmutableSet.of( EMPLOYEE_ID,
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 2.67: Read data - should be able to read all data, but only EMPLOYEE_ID and ADDRESS are non-null.
        System.err.println( " -- READ TEST 2 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result2 = dataApi.getAllEntitiesOfType( NATION_CITIZENS );
        for ( Multimap<FullQualifiedName, Object> entity : result2 ) {
            System.err.println( entity );
        }
        // Cleanup: remove WRITE rights for User.
        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ) ) );

        // Cleanup: remove all current Read/Write rights.
        ps.updateEntityTypesAcls( ImmutableSet.of( new EntityTypeAclRequest().setPrincipal( principal )
                .setAction( Action.REMOVE ).setType( NATION_CITIZENS )
                .setPermissions( EnumSet.of( Permission.READ, Permission.WRITE ) ) ) );
        ps.removeAllPropertyTypesInEntityTypeAcls(
                ImmutableSet.of( NATION_CITIZENS ) );

        // Setup:
        // Give User READ rights for EMPLOYEE_ID, ADDRESS, POSITION in NATION_CITIZENS, as well as READ rights for
        // NATION_CITIZENS
        // User gets the Permission to write NATION_CITIZENS
        ps.updateEntityTypesAcls( ImmutableSet.of(
                new EntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD ).setType( NATION_CITIZENS )
                        .setPermissions( EnumSet.of( Permission.READ, Permission.WRITE ) ) ) );
        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.READ ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.READ ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( POSITION )
                        .setPermissions( EnumSet.of( Permission.READ ) ) ) );

        // Test 3: give Writer WRITE rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in NATION_CITIZENS
        // Expected: Have WRITE rights for all pairs (EMPLOYEE_ID, NATION_CITIZENS),..., (LIFE_EXPECTANCY,
        // NATION_CITIZENS)

        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( POSITION )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ) ) );

        // Test 3.33: Actually write data - all columns should be written.
        createData( 10,
                NATION_CITIZENS,
                Optional.absent(),
                ImmutableSet.of( EMPLOYEE_ID,
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 3.67: Read data - should be unable to read LIFE_EXPECTANCY.
        System.err.println( " -- READ TEST 3 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result3 = dataApi.getAllEntitiesOfType( NATION_CITIZENS );
        for ( Multimap<FullQualifiedName, Object> entity : result3 ) {
            System.err.println( entity );
        }

        // Cleanup: remove WRITE rights for User.

        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( POSITION )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ) ) );

        // Test 4: given Writer WRITE rights for EMPLOYEE_ID, ADDRESS. Inherit rights from NATION_CITIZENS
        // Expected: Only have WRITE rights for (EMPLOYEE_ID, NATION_CITIZENS), (ADDRESS, NATION_CITIZENS)
        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ) ) );

        // Test 4.33: Actually write data - only the columns EMPLOYEE_ID, ADDRESS would be non-empty
        createData( 10,
                NATION_CITIZENS,
                Optional.absent(),
                ImmutableSet.of( EMPLOYEE_ID,
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 4.67: Read data - should be able to read (EMPLOYEE_ID, ADDRESS, POSITION), but only EMPLOYEE_ID and
        // ADDRESS are non-null.
        System.err.println( " -- READ TEST 4 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result4 = dataApi.getAllEntitiesOfType( NATION_CITIZENS );
        for ( Multimap<FullQualifiedName, Object> entity : result4 ) {
            System.err.println( entity );
        }

        // Cleanup: remove WRITE rights for Citizen.
        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ) ) );

        // Cleanup: remove Reader/Writer Role, and all current Read rights.
        ps.updateEntityTypesAcls( ImmutableSet.of( new EntityTypeAclRequest().setPrincipal( principal )
                .setAction( Action.REMOVE ).setType( NATION_CITIZENS )
                .setPermissions( EnumSet.of( Permission.READ, Permission.WRITE ) ) ) );
        ps.removeAllPropertyTypesInEntityTypeAcls(
                ImmutableSet.of( NATION_CITIZENS ) );

    }

    /**
     * WARNING: This test creates garbage data. WARNING: Check tables to see if output is correct.
     */
    private void propertyTypeInEntitySetTest( Principal principal ) {
        System.err.println( "***Property Type In Entity Set Test Starts***" );
        // Give user READ rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in NATION_CITIZENS, as well as
        // READ rights for NATION_SECRET_SERVICE
        // Give user WRITE rights for NATION_SECRET_SERVICE
        ps.updateEntitySetsAcls( ImmutableSet.of( new EntitySetAclRequest().setPrincipal( principal )
                .setAction( Action.ADD ).setName( NATION_SECRET_SERVICE )
                .setPermissions( EnumSet.of( Permission.READ, Permission.WRITE ) ) ) );
        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.READ ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.READ ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( POSITION )
                        .setPermissions( EnumSet.of( Permission.READ ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( EnumSet.of( Permission.READ ) ) ) );

        // Test 1: give Writer WRITE rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in NATION_CITIZENS.
        // Expected: Being able to read and write all data types.

        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( POSITION )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ) ) );

        // Test 1.33: Actually write data to entity type
        createData( 10,
                NATION_CITIZENS,
                Optional.of( NATION_SECRET_SERVICE ),
                ImmutableSet.of( EMPLOYEE_ID,
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 1.67: Read data from entity type - should be able to read all data.
        System.err.println( " -- READ TEST 1 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result1 = dataApi.getAllEntitiesOfEntitySet(
                NATION_SECRET_SERVICE, NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName() );
        for ( Multimap<FullQualifiedName, Object> entity : result1 ) {
            System.err.println( entity );
        }

        // Cleanup: remove WRITE rights for User.
        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( POSITION )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ) ) );

        // Test 2: given User WRITE rights for EMPLOYEE_ID, ADDRESS in NATION_SECRET_SERVICE
        // Expected: Only have WRITE rights for (EMPLOYEE_ID, NATION_SECRET_SERVICE), (ADDRESS, NATION_SECRET_SERVICE)

        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ) ) );

        // Test 2.33: Actually write data - only the columns EMPLOYEE_ID, ADDRESS would be non-empty
        createData( 10,
                NATION_CITIZENS,
                Optional.of( NATION_SECRET_SERVICE ),
                ImmutableSet.of( EMPLOYEE_ID,
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 2.67: Read data - should be able to read all data, but only EMPLOYEE_ID and ADDRESS are non-null.
        System.err.println( " -- READ TEST 2 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result2 = dataApi.getAllEntitiesOfEntitySet(
                NATION_SECRET_SERVICE, NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName() );
        for ( Multimap<FullQualifiedName, Object> entity : result2 ) {
            System.err.println( entity );
        }

        // Cleanup: remove WRITE rights for User.
        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ) ) );

        // Cleanup: remove all current Read/Write rights.
        ps.updateEntitySetsAcls( ImmutableSet.of( new EntitySetAclRequest().setPrincipal( principal )
                .setAction( Action.REMOVE ).setName( NATION_SECRET_SERVICE )
                .setPermissions( EnumSet.of( Permission.READ, Permission.WRITE ) ) ) );
        ps.removeAllPropertyTypesInEntitySetAcls( ImmutableSet.of( NATION_SECRET_SERVICE ) );

        // Setup:
        // Give User READ rights for EMPLOYEE_ID, ADDRESS, POSITION in NATION_SECRET_SERVICE, as well as READ rights for
        // NATION_SECRET_SERVICE
        // User gets the Permission to write NATION_SECRET_SERVICE
        ps.updateEntitySetsAcls( ImmutableSet.of( new EntitySetAclRequest().setPrincipal( principal )
                .setAction( Action.ADD ).setName( NATION_SECRET_SERVICE )
                .setPermissions( EnumSet.of( Permission.READ, Permission.WRITE ) ) ) );
        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.READ ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.READ ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( POSITION )
                        .setPermissions( EnumSet.of( Permission.READ ) ) ) );

        // Test 3: give Writer WRITE rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in NATION_CITIZENS
        // Expected: Have WRITE rights for all pairs (EMPLOYEE_ID, NATION_CITIZENS),..., (LIFE_EXPECTANCY,
        // NATION_CITIZENS)

        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( POSITION )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ) ) );

        // Test 3.33: Actually write data - all columns should be written.
        createData( 10,
                NATION_CITIZENS,
                Optional.of( NATION_SECRET_SERVICE ),
                ImmutableSet.of( EMPLOYEE_ID,
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 3.67: Read data - should be unable to read LIFE_EXPECTANCY.
        System.err.println( " -- READ TEST 3 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result3 = dataApi.getAllEntitiesOfEntitySet(
                NATION_SECRET_SERVICE, NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName() );
        for ( Multimap<FullQualifiedName, Object> entity : result3 ) {
            System.err.println( entity );
        }

        // Cleanup: remove WRITE rights for User.

        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( POSITION )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ) ) );

        // Test 4: given Writer WRITE rights for EMPLOYEE_ID, ADDRESS. Inherit rights from NATION_CITIZENS
        // Expected: Only have WRITE rights for (EMPLOYEE_ID, NATION_CITIZENS), (ADDRESS, NATION_CITIZENS)
        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ) ) );

        // Test 4.33: Actually write data - only the columns EMPLOYEE_ID, ADDRESS would be non-empty
        createData( 10,
                NATION_CITIZENS,
                Optional.of( NATION_SECRET_SERVICE ),
                ImmutableSet.of( EMPLOYEE_ID,
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 4.67: Read data - should be able to read (EMPLOYEE_ID, ADDRESS, POSITION), but only EMPLOYEE_ID and
        // ADDRESS are non-null.
        System.err.println( " -- READ TEST 4 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result4 = dataApi.getAllEntitiesOfEntitySet(
                NATION_SECRET_SERVICE, NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName() );
        for ( Multimap<FullQualifiedName, Object> entity : result4 ) {
            System.err.println( entity );
        }
        // Cleanup: remove WRITE rights for Citizen.
        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.WRITE ) ) ) );
        // Cleanup: remove Reader/Writer Role, and all current Read rights.
        ps.updateEntitySetsAcls( ImmutableSet.of( new EntitySetAclRequest().setPrincipal( principal )
                .setAction( Action.REMOVE ).setName( NATION_SECRET_SERVICE )
                .setPermissions( EnumSet.of( Permission.READ, Permission.WRITE ) ) ) );
        ps.removeAllPropertyTypesInEntitySetAcls( ImmutableSet.of( NATION_SECRET_SERVICE ) );
    }

    private void createData(
            int dataLength,
            FullQualifiedName entityTypeFqn,
            Optional<String> entitySetName,
            Set<FullQualifiedName> includedProperties ) {
        Random rand = new Random();

        Set<SetMultimap<FullQualifiedName, Object>> entities = new HashSet<>();
        for ( int i = 0; i < dataLength; i++ ) {
            SetMultimap<FullQualifiedName, Object> entity = HashMultimap.create();

            entity.put( EMPLOYEE_ID, UUID.randomUUID() );

            if ( includedProperties.contains( LIFE_EXPECTANCY ) ) {
                entity.put( LIFE_EXPECTANCY, rand.nextInt( 100 ) );
            }

            if ( includedProperties.contains( ADDRESS ) ) {
                entity.put( ADDRESS, RandomStringUtils.randomAlphanumeric( 10 ) );
            }

            if ( includedProperties.contains( POSITION ) ) {
                entity.put( POSITION, RandomStringUtils.randomAlphabetic( 6 ) );
            }
            entities.add( entity );
        }

        CreateEntityRequest createEntityRequest = new CreateEntityRequest(
                entitySetName,
                entityTypeFqn,
                entities,
                Optional.absent(),
                Optional.absent() );

        dataApi.createEntityData( createEntityRequest );
    }

    private void requestAccess() {
        System.err.println( "---TEST 1---" );
        // Test 1: God creates Entity Sets Hombres, Mujeres, and give Citizen DISCOVER rights
        // Citizen requests access to them
        // Expected: Citizens' sent request list should have Hombres and Mujeres
        String HOMBRES = "hombres";
        String MUJERES = "mujeres";

        EntitySet hombres = new EntitySet().setType( NATION_CITIZENS )
                .setName( HOMBRES )
                .setTitle( "Every nation would have some" );
        EntitySet mujeres = new EntitySet().setType( NATION_CITIZENS )
                .setName( MUJERES )
                .setTitle( "Every nation would have some" );
        edmService.createEntitySet( hombres );
        edmService.createEntitySet( mujeres );

        ps.updateEntitySetsAcls(
                ImmutableSet.of( new EntitySetAclRequest().setPrincipal( ROLE_USER ).setAction( Action.ADD )
                        .setName( HOMBRES ).setPermissions( EnumSet.of( Permission.DISCOVER ) ) ) );
        ps.updateEntitySetsAcls(
                ImmutableSet.of( new EntitySetAclRequest().setPrincipal( ROLE_USER ).setAction( Action.ADD )
                        .setName( MUJERES ).setPermissions( EnumSet.of( Permission.DISCOVER ) ) ) );

        // Request for HOMBRES: request READ access the entity set itself, no specific property type
        ps.addPermissionsRequestForPropertyTypesInEntitySet( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( ROLE_USER ).setAction( Action.REQUEST )
                        .setName( HOMBRES ).setPermissions( EnumSet.of( Permission.READ ) ) ) );
        // Request for MUJERES: request READ,WRITE access for Property Type ADDRESS
        ps.addPermissionsRequestForPropertyTypesInEntitySet( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( USER_USER ).setAction( Action.REQUEST )
                        .setName( MUJERES ).setPropertyType( ADDRESS )
                        .setPermissions( EnumSet.of( Permission.READ, Permission.WRITE ) ) ) );

        System.err.println( "--- TEST FOR GETTING ALL SENT REQUEST --- " );
        System.err.println( ps.getAllSentRequestsForPermissions( null ) );

        System.err.println( "--- TEST FOR GETTING SENT REQUEST FOR HOMBRES --- " );
        System.err.println( ps.getAllSentRequestsForPermissions( HOMBRES ) );

        System.err.println( "---TEST 2---" );
        // Test 2: Citizen removes Request for Hombres
        // Expected: Citizens' sent request list should have only Mujeres

        ps.getAllSentRequestsForPermissions( HOMBRES )
                .forEach( request -> ps.removePermissionsRequestForEntitySet( request.getRequestId() ) );

        System.err.println( "--- TEST FOR GETTING ALL SENT REQUEST --- " );
        System.err.println( ps.getAllSentRequestsForPermissions( null ) );

        System.err.println( "--- TEST FOR GETTING SENT REQUEST FOR HOMBRES --- " );
        System.err.println( ps.getAllSentRequestsForPermissions( HOMBRES ) );

        System.err.println( "---TEST 3---" );
        // Test 3: Citizens create Entity Sets Cate, Doge. A few request accesses were made to them.
        // Expected: Citizens' received request list should have Cate and Doge.

        String CATE = "kryptocate";
        String DOGE = "kryptodoge";

        EntitySet cate = new EntitySet().setType( NATION_CITIZENS )
                .setName( CATE )
                .setTitle( "Every reddit would have some" );
        EntitySet doge = new EntitySet().setType( NATION_CITIZENS )
                .setName( DOGE )
                .setTitle( "Every reddit would have some" );
        edmApi.postEntitySets( ImmutableSet.of( cate, doge ) );
        // Sanity check for ownership
        System.err.println( "--- SANITY TEST FOR OWNERSHIP OF CATE AND DOGE --- " );
        System.err.println( edmApi.getEntitySets( null ) );

        // Add permissions request
        permissionsService.addPermissionsRequestForPropertyTypeInEntitySet(
                "redditUser1", ROLE_USER, CATE, ADDRESS, EnumSet.of( Permission.READ ) );
        permissionsService.addPermissionsRequestForPropertyTypeInEntitySet(
                "redditUser314",
                USER_USER,
                DOGE,
                LIFE_EXPECTANCY,
                EnumSet.of( Permission.READ, Permission.WRITE, Permission.DISCOVER ) );

        System.err.println( "--- TEST FOR GETTING ALL RECEIVED REQUEST --- " );
        System.err.println( ps.getAllReceivedRequestsForPermissions( null ) );

        System.err.println( "--- TEST FOR GETTING RECEIVED REQUEST FOR CATE --- " );
        System.err.println( ps.getAllReceivedRequestsForPermissions( CATE ) );

        System.err.println( "--- TEST FOR GETTING RECEIVED REQUEST FOR HOMBRES --- " );
        try {
            System.err.println( ps.getAllReceivedRequestsForPermissions( HOMBRES ) );
        } catch ( Exception e ) {
            // 404 thrown by ExceptionHandler would be caught by retrofit, so it's NotFoundException rather than custom
            // ResourceNotFoundException
            Assert.assertTrue( e instanceof NotFoundException );
        }

        requestAccessCleanup();
    }

    private void requestAccessCleanup() {
        // Remove unattended PermissionsRequest to avoid pollution.
        ps.getAllSentRequestsForPermissions( null )
                .forEach( request -> ps.removePermissionsRequestForEntitySet( request.getRequestId() ) );
        ps.getAllReceivedRequestsForPermissions( null )
                .forEach( request -> ps.removePermissionsRequestForEntitySet( request.getRequestId() ) );
    }
}
