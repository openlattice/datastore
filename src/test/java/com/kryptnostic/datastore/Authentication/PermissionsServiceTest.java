package com.kryptnostic.datastore.Authentication;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

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
import com.geekbeast.rhizome.tests.bootstrap.DefaultErrorHandler;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.Datastore;
import com.kryptnostic.datastore.Permission;
import com.kryptnostic.datastore.Principal;
import com.kryptnostic.datastore.PrincipalType;
import com.kryptnostic.datastore.services.DataApi;
import com.kryptnostic.datastore.services.EdmApi;
import com.kryptnostic.datastore.services.PermissionsApi;
import com.kryptnostic.datastore.services.requests.Action;
import com.kryptnostic.datastore.services.requests.CreateEntityRequest;
import com.kryptnostic.datastore.services.requests.EntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.EntityTypeAclRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntityTypeAclRequest;
import com.kryptnostic.rhizome.converters.RhizomeConverter;

import digital.loom.rhizome.authentication.AuthenticationTest;
import digital.loom.rhizome.configuration.auth0.Auth0Configuration;
import retrofit.RequestInterceptor;
import retrofit.RestAdapter;

public class PermissionsServiceTest {

    protected static final Principal         ROLE_USER             = new Principal( PrincipalType.ROLE, "user" );
    protected static final Principal         USER_USER             = new Principal( PrincipalType.USER, "support@kryptnostic.com" );

    protected static final String            NATION_NAMESPACE      = "us";
    protected static final FullQualifiedName NATION_SCHEMA         = new FullQualifiedName(
            NATION_NAMESPACE,
            "schema" );                                                                                          // schema

    protected static final FullQualifiedName NATION_CITIZENS       = new FullQualifiedName(
            NATION_NAMESPACE,
            "citizens" );                                                                                        // entity
                                                                                                                 // type
    protected static final String            NATION_SECRET_SERVICE = "secret-service";                           // entity
                                                                                                                 // set
                                                                                                                 // name

    protected static final FullQualifiedName EMPLOYEE_ID           = new FullQualifiedName(
            NATION_NAMESPACE,
            "employee-id" );

    protected static final FullQualifiedName LIFE_EXPECTANCY       = new FullQualifiedName(
            NATION_NAMESPACE,
            "life-expectancy" );                                                                                 // property
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
            "spied-on" );                                                                                        // property
                                                                                                                 // type

    private static final Logger              logger                = LoggerFactory.getLogger( Auth0Test.class );
    protected static final Datastore         ds                    = new Datastore();
    protected static Auth0Configuration      configuration;
    protected static Auth0                   auth0;
    protected static AuthenticationAPIClient client;
    protected static DataApi                 dataApi;
    protected static EdmApi                  edmApi;
    protected static PermissionsApi          ps;
    protected static RestAdapter             dataServiceRestAdapter;

    @BeforeClass
    public static void init() throws Exception {
        ds.start( "cassandra" );
        configuration = ds.getContext().getBean( Auth0Configuration.class );
        auth0 = new Auth0( configuration.getClientId(), configuration.getDomain() );
        client = auth0.newAuthenticationAPIClient();
        String jwtToken = AuthenticationTest.authenticate().getLeft().getIdToken();
        dataServiceRestAdapter = new RestAdapter.Builder()
                .setEndpoint( "http://localhost:8080/ontology" )
                .setRequestInterceptor(
                        (RequestInterceptor) facade -> facade.addHeader( "Authorization", "Bearer " + jwtToken ) )
                .setConverter( new RhizomeConverter() )
                .setErrorHandler( new DefaultErrorHandler() )
                .setLogLevel( RestAdapter.LogLevel.FULL )
                .setLog( new RestAdapter.Log() {
                    @Override
                    public void log( String msg ) {
                        logger.debug( msg.replaceAll( "%", "[percent]" ) );
                    }
                } )
                .build();
        dataApi = dataServiceRestAdapter.create( DataApi.class );
        edmApi = dataServiceRestAdapter.create( EdmApi.class );
        ps = dataServiceRestAdapter.create( PermissionsApi.class );
    }

    @Test
    public void permissionsServiceTest() {
        createTypes();
        
        System.out.println( "*********************" );
        System.out.println( "ROLE TESTS START!" );
        System.out.println( "*********************" );
        
        entityTypeTest( ROLE_USER );
        entitySetTest( ROLE_USER);
        propertyTypeInEntityTypeTest( ROLE_USER );
        propertyTypeInEntitySetTest( ROLE_USER);
        
        System.out.println( "*********************" );
        System.out.println( "ROLE TESTS END!" );
        System.out.println( "*********************" );

        System.out.println( "*********************" );
        System.out.println( "USER TESTS START!" );
        System.out.println( "*********************" );
        
        entityTypeTest( USER_USER );
        entitySetTest( USER_USER);
        propertyTypeInEntityTypeTest( USER_USER );
        propertyTypeInEntitySetTest( USER_USER);
        
        System.out.println( "*********************" );
        System.out.println( "USER TESTS END!" );
        System.out.println( "*********************" );

    }

    @AfterClass
    public static void cleanUp() {
        // Give permissions
        ps.updateEntityTypesAcls(
                ImmutableSet.of( new EntityTypeAclRequest().setPrincipal( ROLE_USER ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPermissions( ImmutableSet.of( Permission.ALTER ) ) ) );

        // Delete
        edmApi.deletePropertyType( EMPLOYEE_ID.getNamespace(), EMPLOYEE_ID.getName() );
        edmApi.deletePropertyType( LIFE_EXPECTANCY.getNamespace(), LIFE_EXPECTANCY.getName() );
        edmApi.deletePropertyType( ADDRESS.getNamespace(), ADDRESS.getName() );
        edmApi.deletePropertyType( POSITION.getNamespace(), POSITION.getName() );
        edmApi.deletePropertyType( SPIED_ON.getNamespace(), SPIED_ON.getName() );

        edmApi.deleteEntityType( NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName() );

        System.out.println( "*****And the nation fell out of sight..*****" );
        System.out.println( "*****Test ends!*****" );
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

        // God creates entity type Citizen
        EntityType citizens = new EntityType().setNamespace( NATION_NAMESPACE ).setName( NATION_CITIZENS.getName() )
                .setKey( ImmutableSet.of( EMPLOYEE_ID ) )
                .setProperties( ImmutableSet.of(
                        EMPLOYEE_ID,
                        LIFE_EXPECTANCY,
                        ADDRESS,
                        POSITION ) );
        edmApi.postEntityType( citizens );

        // God creates entity set Secret Service
        EntitySet secretService = new EntitySet().setType( NATION_CITIZENS )
                .setName( NATION_SECRET_SERVICE )
                .setTitle( "Every nation would have one" );
        edmApi.postEntitySets( ImmutableSet.of( secretService ) );
    }

    private EntityType entityTypeMetadataLookup( FullQualifiedName entityTypeFqn ) {
        EntityType result = edmApi.getEntityType( entityTypeFqn.getNamespace(), entityTypeFqn.getName() );
        System.out.println( "Getting Entity Types metadata for " + entityTypeFqn + ": " + result );
        return result;
    }

    private void entityTypeTest( Principal principal ) {
        System.out.println( "***Entity Type Test starts***" );
        // Get metadata for NATION_CITIZENS
        entityTypeMetadataLookup( NATION_CITIZENS );

        System.out.println( "Test 1 Starts!" );
        // Test 1: Citizen has ALTER permission for NATION_CITIZENS
        // Expected: RandomGuy can add/remove property types from NATION_CITIZENS.

        ps.updateEntityTypesAcls(
                ImmutableSet.of( new EntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPermissions( ImmutableSet.of( Permission.ALTER ) ) ) );

        // Current setting is everyone can create types
        PropertyType spiedOn = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( SPIED_ON.getName() )
                .setDatatype( EdmPrimitiveTypeKind.Boolean ).setMultiplicity( 0 );
        edmApi.createPropertyType( spiedOn );

        edmApi.addPropertyTypesToEntityType( NATION_CITIZENS.getNamespace(),
                NATION_CITIZENS.getName(),
                ImmutableSet.of( SPIED_ON ) );

        System.out.println( "Expected: Property SPIED_ON is added to entity Type" );
        entityTypeMetadataLookup( NATION_CITIZENS );

        System.out.println( "Expected: Property SPIED_ON is removed from entity Type" );
        edmApi.removePropertyTypesFromEntityType( NATION_CITIZENS.getNamespace(),
                NATION_CITIZENS.getName(),
                ImmutableSet.of( SPIED_ON ) );
        entityTypeMetadataLookup( NATION_CITIZENS );

        ps.updateEntityTypesAcls(
                ImmutableSet.of( new EntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPermissions( ImmutableSet.of( Permission.ALTER ) ) ) );

        System.out.println( "Test 2 Starts!" );
        // Test 2: Citizen has ALTER permission for NATION_CITIZENS
        // Expected: RandomGuy can delete the entity type NATION_CITIZENS. (Which also means the entity sets inside are
        // deleted)
        ps.updateEntityTypesAcls(
                ImmutableSet.of( new EntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPermissions( ImmutableSet.of( Permission.ALTER ) ) ) );

        edmApi.deleteEntityType( NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName() );
        System.out.println( "Expected: Entity Type NATION_CITIZENS is removed." );
        System.out.println( "Print all entity types:" );
        for ( EntityType entityType : edmApi.getEntityTypes() ) {
            System.out.println( entityType );
        }
        System.out.println( "Printing finished." );

        entityTypeTestCleanup();
    }

    private void entityTypeTestCleanup() {
        System.out.println( " *** Entity Type Test Clean Up Happening *** " );
        // God needs to create the entity type and entity set back
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
        edmApi.postEntitySets( ImmutableSet.of( secretService ) );

        System.out.println( " *** Entity Type Test Clean Up Finished *** " );
    }

    private void entitySetTest( Principal principal ) {
        System.out.println( "***Entity Set Test starts***" );

        System.out.println( "Test 1 Starts!" );
        // Test 1: Citizen is given the DISCOVER permission for Secret Service; and the right got removed after.
        // Expected: RandomGuy can see the metadata for Secret Service; and cannot after.
        ps.updateEntitySetsAcls(
                ImmutableSet.of( new EntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPermissions( ImmutableSet.of( Permission.DISCOVER ) ) ) );
        Assert.assertNotEquals( 0, Iterables.size( edmApi.getEntitySets() ) );

        ps.updateEntitySetsAcls(
                ImmutableSet.of( new EntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPermissions( ImmutableSet.of( Permission.DISCOVER ) ) ) );
        Assert.assertEquals( 0, Iterables.size( edmApi.getEntitySets() ) );
    }

    /**
     * WARNING: This test creates garbage data. WARNING: Check tables to see if output is correct.
     */
    private void propertyTypeInEntityTypeTest( Principal principal ) {
        System.out.println( "***Property Type In Entity Type Test Starts***" );
        // Give user READ rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in NATION_CITIZENS, as well as
        // READ rights for NATION_CITIZENS
        // Give user WRITE rights for NATION_CITIZENS
        ps.updateEntityTypesAcls( ImmutableSet.of(
                new EntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD ).setType( NATION_CITIZENS )
                        .setPermissions( ImmutableSet.of( Permission.READ, Permission.WRITE ) ) ) );
        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.READ ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.READ ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( POSITION )
                        .setPermissions( ImmutableSet.of( Permission.READ ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( ImmutableSet.of( Permission.READ ) ) ) );

        // Test 1: give Writer WRITE rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in NATION_CITIZENS.
        // Expected: Being able to read and write all data types.

        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( POSITION )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ) ) );

        // Test 1.33: Actually write data to entity type
        createData( 10,
                NATION_CITIZENS,
                Optional.absent(),
                ImmutableSet.of( EMPLOYEE_ID,
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 1.67: Read data from entity type - should be able to read all data.
        System.out.println( " -- READ TEST 1 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result1 = dataApi.getAllEntitiesOfType( NATION_CITIZENS );
        for ( Multimap<FullQualifiedName, Object> entity : result1 ) {
            System.out.println( entity );
        }

        // Cleanup: remove WRITE rights for User.
        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( POSITION )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ) ) );

        // Test 2: given User WRITE rights for EMPLOYEE_ID, ADDRESS in NATION_CITIZENS
        // Expected: Only have WRITE rights for (EMPLOYEE_ID, NATION_CITIZENS), (ADDRESS, NATION_CITIZENS)

        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ) ) );

        // Test 2.33: Actually write data - only the columns EMPLOYEE_ID, ADDRESS would be non-empty
        createData( 10,
                NATION_CITIZENS,
                Optional.absent(),
                ImmutableSet.of( EMPLOYEE_ID,
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 2.67: Read data - should be able to read all data, but only EMPLOYEE_ID and ADDRESS are non-null.
        System.out.println( " -- READ TEST 2 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result2 = dataApi.getAllEntitiesOfType( NATION_CITIZENS );
        for ( Multimap<FullQualifiedName, Object> entity : result2 ) {
            System.out.println( entity );
        }
        // Cleanup: remove WRITE rights for User.
        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ) ) );

        // Cleanup: remove all current Read/Write rights.
        ps.updateEntityTypesAcls( ImmutableSet.of( new EntityTypeAclRequest().setPrincipal( principal )
                .setAction( Action.REMOVE ).setType( NATION_CITIZENS )
                .setPermissions( ImmutableSet.of( Permission.READ, Permission.WRITE ) ) ) );
        ps.removeAllPropertyTypesInEntityTypeAcls(
                ImmutableSet.of( NATION_CITIZENS ) );

        // Setup:
        // Give User READ rights for EMPLOYEE_ID, ADDRESS, POSITION in NATION_CITIZENS, as well as READ rights for
        // NATION_CITIZENS
        // User gets the Permission to write NATION_CITIZENS
        ps.updateEntityTypesAcls( ImmutableSet.of(
                new EntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD ).setType( NATION_CITIZENS )
                        .setPermissions( ImmutableSet.of( Permission.READ, Permission.WRITE ) ) ) );
        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.READ ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.READ ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( POSITION )
                        .setPermissions( ImmutableSet.of( Permission.READ ) ) ) );

        // Test 3: give Writer WRITE rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in NATION_CITIZENS
        // Expected: Have WRITE rights for all pairs (EMPLOYEE_ID, NATION_CITIZENS),..., (LIFE_EXPECTANCY,
        // NATION_CITIZENS)

        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( POSITION )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ) ) );

        // Test 3.33: Actually write data - all columns should be written.
        createData( 10,
                NATION_CITIZENS,
                Optional.absent(),
                ImmutableSet.of( EMPLOYEE_ID,
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 3.67: Read data - should be unable to read LIFE_EXPECTANCY.
        System.out.println( " -- READ TEST 3 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result3 = dataApi.getAllEntitiesOfType( NATION_CITIZENS );
        for ( Multimap<FullQualifiedName, Object> entity : result3 ) {
            System.out.println( entity );
        }

        // Cleanup: remove WRITE rights for User.

        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( POSITION )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ) ) );

        // Test 4: given Writer WRITE rights for EMPLOYEE_ID, ADDRESS. Inherit rights from NATION_CITIZENS
        // Expected: Only have WRITE rights for (EMPLOYEE_ID, NATION_CITIZENS), (ADDRESS, NATION_CITIZENS)
        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ) ) );

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
        System.out.println( " -- READ TEST 4 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result4 = dataApi.getAllEntitiesOfType( NATION_CITIZENS );
        for ( Multimap<FullQualifiedName, Object> entity : result4 ) {
            System.out.println( entity );
        }

        // Cleanup: remove WRITE rights for Citizen.
        ps.updatePropertyTypeInEntityTypeAcls( ImmutableSet.of(
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntityTypeAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setType( NATION_CITIZENS ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ) ) );

        // Cleanup: remove Reader/Writer Role, and all current Read rights.
        ps.updateEntityTypesAcls( ImmutableSet.of( new EntityTypeAclRequest().setPrincipal( principal )
                .setAction( Action.REMOVE ).setType( NATION_CITIZENS )
                .setPermissions( ImmutableSet.of( Permission.READ, Permission.WRITE ) ) ) );
        ps.removeAllPropertyTypesInEntityTypeAcls(
                ImmutableSet.of( NATION_CITIZENS ) );
    }

    /**
     * WARNING: This test creates garbage data. WARNING: Check tables to see if output is correct.
     */
    private void propertyTypeInEntitySetTest( Principal principal ) {
        System.out.println( "***Property Type In Entity Set Test Starts***" );
        // Give user READ rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in NATION_CITIZENS, as well as
        // READ rights for NATION_SECRET_SERVICE
        // Give user WRITE rights for NATION_SECRET_SERVICE
        ps.updateEntitySetsAcls( ImmutableSet.of( new EntitySetAclRequest().setPrincipal( principal )
                .setAction( Action.ADD ).setName( NATION_SECRET_SERVICE )
                .setPermissions( ImmutableSet.of( Permission.READ, Permission.WRITE ) ) ) );
        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.READ ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.READ ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( POSITION )
                        .setPermissions( ImmutableSet.of( Permission.READ ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( ImmutableSet.of( Permission.READ ) ) ) );

        // Test 1: give Writer WRITE rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in NATION_CITIZENS.
        // Expected: Being able to read and write all data types.

        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( POSITION )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ) ) );

        // Test 1.33: Actually write data to entity type
        createData( 10,
                NATION_CITIZENS,
                Optional.of( NATION_SECRET_SERVICE ),
                ImmutableSet.of( EMPLOYEE_ID,
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 1.67: Read data from entity type - should be able to read all data.
        System.out.println( " -- READ TEST 1 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result1 = dataApi.getAllEntitiesOfEntitySet(
                NATION_SECRET_SERVICE, NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName() );
        for ( Multimap<FullQualifiedName, Object> entity : result1 ) {
            System.out.println( entity );
        }

        // Cleanup: remove WRITE rights for User.
        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( POSITION )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ) ) );

        // Test 2: given User WRITE rights for EMPLOYEE_ID, ADDRESS in NATION_SECRET_SERVICE
        // Expected: Only have WRITE rights for (EMPLOYEE_ID, NATION_SECRET_SERVICE), (ADDRESS, NATION_SECRET_SERVICE)

        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ) ) );

        // Test 2.33: Actually write data - only the columns EMPLOYEE_ID, ADDRESS would be non-empty
        createData( 10,
                NATION_CITIZENS,
                Optional.of( NATION_SECRET_SERVICE ),
                ImmutableSet.of( EMPLOYEE_ID,
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 2.67: Read data - should be able to read all data, but only EMPLOYEE_ID and ADDRESS are non-null.
        System.out.println( " -- READ TEST 2 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result2 = dataApi.getAllEntitiesOfEntitySet(
                NATION_SECRET_SERVICE, NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName() );
        for ( Multimap<FullQualifiedName, Object> entity : result2 ) {
            System.out.println( entity );
        }

        // Cleanup: remove WRITE rights for User.
        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ) ) );

        // Cleanup: remove all current Read/Write rights.
        ps.updateEntitySetsAcls( ImmutableSet.of( new EntitySetAclRequest().setPrincipal( principal )
                .setAction( Action.REMOVE ).setName( NATION_SECRET_SERVICE )
                .setPermissions( ImmutableSet.of( Permission.READ, Permission.WRITE ) ) ) );
        ps.removeAllPropertyTypesInEntitySetAcls( ImmutableSet.of( NATION_SECRET_SERVICE ) );

        // Setup:
        // Give User READ rights for EMPLOYEE_ID, ADDRESS, POSITION in NATION_SECRET_SERVICE, as well as READ rights for
        // NATION_SECRET_SERVICE
        // User gets the Permission to write NATION_SECRET_SERVICE
        ps.updateEntitySetsAcls( ImmutableSet.of( new EntitySetAclRequest().setPrincipal( principal )
                .setAction( Action.ADD ).setName( NATION_SECRET_SERVICE )
                .setPermissions( ImmutableSet.of( Permission.READ, Permission.WRITE ) ) ) );
        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.READ ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.READ ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( POSITION )
                        .setPermissions( ImmutableSet.of( Permission.READ ) ) ) );

        // Test 3: give Writer WRITE rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in NATION_CITIZENS
        // Expected: Have WRITE rights for all pairs (EMPLOYEE_ID, NATION_CITIZENS),..., (LIFE_EXPECTANCY,
        // NATION_CITIZENS)

        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( POSITION )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ) ) );

        // Test 3.33: Actually write data - all columns should be written.
        createData( 10,
                NATION_CITIZENS,
                Optional.of( NATION_SECRET_SERVICE ),
                ImmutableSet.of( EMPLOYEE_ID,
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 3.67: Read data - should be unable to read LIFE_EXPECTANCY.
        System.out.println( " -- READ TEST 3 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result3 = dataApi.getAllEntitiesOfEntitySet(
                NATION_SECRET_SERVICE, NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName() );
        for ( Multimap<FullQualifiedName, Object> entity : result3 ) {
            System.out.println( entity );
        }

        // Cleanup: remove WRITE rights for User.

        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( POSITION )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( LIFE_EXPECTANCY )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ) ) );

        // Test 4: given Writer WRITE rights for EMPLOYEE_ID, ADDRESS. Inherit rights from NATION_CITIZENS
        // Expected: Only have WRITE rights for (EMPLOYEE_ID, NATION_CITIZENS), (ADDRESS, NATION_CITIZENS)
        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.ADD )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ) ) );

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
        System.out.println( " -- READ TEST 4 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result4 = dataApi.getAllEntitiesOfEntitySet(
                NATION_SECRET_SERVICE, NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName() );
        for ( Multimap<FullQualifiedName, Object> entity : result4 ) {
            System.out.println( entity );
        }
        // Cleanup: remove WRITE rights for Citizen.
        ps.updatePropertyTypeInEntitySetAcls( ImmutableSet.of(
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( EMPLOYEE_ID )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ),
                new PropertyTypeInEntitySetAclRequest().setPrincipal( principal ).setAction( Action.REMOVE )
                        .setName( NATION_SECRET_SERVICE ).setPropertyType( ADDRESS )
                        .setPermissions( ImmutableSet.of( Permission.WRITE ) ) ) );
        // Cleanup: remove Reader/Writer Role, and all current Read rights.
        ps.updateEntitySetsAcls( ImmutableSet.of( new EntitySetAclRequest().setPrincipal( principal )
                .setAction( Action.REMOVE ).setName( NATION_SECRET_SERVICE )
                .setPermissions( ImmutableSet.of( Permission.READ, Permission.WRITE ) ) ) );
        ps.removeAllPropertyTypesInEntitySetAcls( ImmutableSet.of( NATION_SECRET_SERVICE ) );
    }

    private void createData(
            int dataLength,
            FullQualifiedName entityTypeFqn,
            Optional<String> entitySetName,
            Set<FullQualifiedName> includedProperties ) {
        Random rand = new Random();

        Set<Multimap<FullQualifiedName, Object>> entities = new HashSet<>();
        for ( int i = 0; i < dataLength; i++ ) {
            Multimap<FullQualifiedName, Object> entity = HashMultimap.create();

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
}
