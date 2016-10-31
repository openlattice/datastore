package com.kryptnostic.datastore.edm;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.Permission;
import com.kryptnostic.datastore.services.requests.CreateEntityRequest;

public class BabyAclTest extends BootstrapDatastoreWithCassandra {

    protected static final String            ROLE_READER           = "reader";
    protected static final String            ROLE_WRITER           = "writer";
    protected static final String            ROLE_GOVERNOR         = "governor";
    protected static final String            ROLE_CITIZEN          = "citizen";
    protected static final User              USER_RANDOMGUY        = new User(
            "RANDOM_GUY",
            new ArrayList<>( Arrays.asList( ROLE_CITIZEN ) ) );

    protected static final String            NATION_NAMESPACE      = "us";
    protected static final FullQualifiedName NATION_SCHEMA         = new FullQualifiedName(
            NATION_NAMESPACE,
            "schema" );                                                                    // schema

    protected static final FullQualifiedName NATION_CITIZENS       = new FullQualifiedName(
            NATION_NAMESPACE,
            "citizens" );                                                                  // entity type
    protected static final String            NATION_SECRET_SERVICE = "secret-service";     // entity set name

    protected static final FullQualifiedName LIFE_EXPECTANCY       = new FullQualifiedName(
            NATION_NAMESPACE,
            "life-expectancy" );                                                           // property type
    protected static final FullQualifiedName ADDRESS               = new FullQualifiedName(
            NATION_NAMESPACE,
            "address" );                                                                   // property type
    protected static final FullQualifiedName POSITION              = new FullQualifiedName(
            NATION_NAMESPACE,
            "position" );                                                                  // property type

    protected static final FullQualifiedName SPIED_ON              = new FullQualifiedName(
            NATION_NAMESPACE,
            "spied-on" );                                                                  // property type

    /**
     * Start of JUnit Tests
     */
    // Create property types/entity types/entity sets/schemas with different acl's, and modify existing types to change
    // acl's
    // Schema should only show those the user has rights to see.
    @BeforeClass
    public static void initializeAcl() {
        createTypes();
        grantRights();
    }

    @Test
    public void Test() {
        System.out.println( "Test starts!" );
        entityTypeTest();
        entitySetTest();
        propertyTypeInEntityTypeTest();
        propertyTypeInEntitySetTest();
    }

    // Delete created property types/entity types/entity sets/schemas - Acl tables should update correspondingly.
    @AfterClass
    public static void resetAcl() {
        setUser( USER_GOD );
        // Property Types
        dms.deletePropertyType( LIFE_EXPECTANCY );
        dms.deletePropertyType( ADDRESS );
        dms.deletePropertyType( POSITION );
        dms.deletePropertyType( SPIED_ON );

        dms.deleteEntityType( NATION_CITIZENS );

        System.out.println( "*****And the nation fell out of sight..*****" );
        System.out.println( "*****Test ends!*****" );
    }

    private static void createTypes() {
        // God creates property types Address, Position
        PropertyType lifeExpectancy = new PropertyType().setNamespace( NATION_NAMESPACE )
                .setName( LIFE_EXPECTANCY.getName() )
                .setDatatype( EdmPrimitiveTypeKind.Int32 ).setMultiplicity( 0 );
        PropertyType address = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( ADDRESS.getName() )
                .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 );
        PropertyType position = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( POSITION.getName() )
                .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 );
        dms.createPropertyType( lifeExpectancy );
        dms.createPropertyType( address );
        dms.createPropertyType( position );

        // God creates entity type Citizen
        EntityType citizens = new EntityType().setNamespace( NATION_NAMESPACE ).setName( NATION_CITIZENS.getName() )
                .setKey( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ) ) )
                .setProperties( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        LIFE_EXPECTANCY,
                        ADDRESS,
                        POSITION ) );
        dms.createEntityType( citizens );

        // God creates entity set Secret Service
        EntitySet secretService = new EntitySet().setType( NATION_CITIZENS )
                .setName( NATION_SECRET_SERVICE )
                .setTitle( "Every nation would have one" );
        dms.createEntitySet( secretService );
    }

    private static void grantRights() {
        // God grants rights to roles
        setUser( USER_GOD );

        // Entity Types
        ps.addPermissionsForEntityType( ROLE_GOVERNOR,
                NATION_CITIZENS,
                ImmutableSet.of( Permission.ALTER ) );
        ps.addPermissionsForEntityType( ROLE_READER,
                NATION_CITIZENS,
                ImmutableSet.of( Permission.READ ) );
        ps.addPermissionsForEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                ImmutableSet.of( Permission.WRITE ) );

        // Entity Set
        ps.addPermissionsForEntitySet( ROLE_GOVERNOR,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ImmutableSet.of( Permission.ALTER ) );
        ps.addPermissionsForEntitySet( ROLE_READER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ImmutableSet.of( Permission.READ ) );
        ps.addPermissionsForEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ImmutableSet.of( Permission.WRITE ) );
    }

    private EntityType entityTypeMetadataLookup( User user, FullQualifiedName entityTypeFqn ) {
        setUser( user );
        EntityType result = dms.getEntityType( entityTypeFqn );
        System.out.println( user.getName() + " getting Entity Types metadata for " + entityTypeFqn + ": " + result );
        return result;
    }

    private void entityTypeTest() {
        System.out.println( "***Entity Type Test starts***" );
        // Test 1: Citizen has ALTER permission for NATION_CITIZENS
        // Expected: RandomGuy can add/remove property types from NATION_CITIZENS.
        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of( Permission.ALTER ) );

        setUser( USER_RANDOMGUY );
        // Current setting is everyone can create types
        PropertyType spiedOn = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( SPIED_ON.getName() )
                .setDatatype( EdmPrimitiveTypeKind.Boolean ).setMultiplicity( 0 );
        dms.createPropertyType( spiedOn );

        dms.addPropertyTypesToEntityType( NATION_CITIZENS.getNamespace(),
                NATION_CITIZENS.getName(),
                ImmutableSet.of( SPIED_ON ) );
        entityTypeMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS );
        dms.removePropertyTypesFromEntityType( NATION_CITIZENS.getNamespace(),
                NATION_CITIZENS.getName(),
                ImmutableSet.of( SPIED_ON ) );

        ps.removePermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of( Permission.ALTER ) );

        // Test 2: Citizen has ALTER permission for NATION_CITIZENS
        // Expected: RandomGuy can delete the entity type NATION_CITIZENS. (Which also means the entity sets inside are
        // deleted)
        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of( Permission.ALTER ) );

        setUser( USER_RANDOMGUY );
        dms.deleteEntityType( NATION_CITIZENS );

        ps.removePermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of( Permission.ALTER ) );
        entityTypeTestCleanup();
    }

    private void entityTypeTestCleanup() {
        setUser( USER_GOD );
        // God needs to create the entity type and entity set back, and grant permissions again
        EntityType citizens = new EntityType().setNamespace( NATION_NAMESPACE ).setName( NATION_CITIZENS.getName() )
                .setKey( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ) ) )
                .setProperties( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        LIFE_EXPECTANCY,
                        ADDRESS,
                        POSITION ) );
        dms.createEntityType( citizens );

        EntitySet secretService = new EntitySet().setType( NATION_CITIZENS )
                .setName( NATION_SECRET_SERVICE )
                .setTitle( "Every nation would have one" );
        dms.createEntitySet( secretService );

        // Entity Types
        ps.addPermissionsForEntityType( ROLE_GOVERNOR,
                NATION_CITIZENS,
                ImmutableSet.of( Permission.ALTER ) );
        ps.addPermissionsForEntityType( ROLE_READER,
                NATION_CITIZENS,
                ImmutableSet.of( Permission.READ ) );
        ps.addPermissionsForEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                ImmutableSet.of( Permission.WRITE ) );

        // Entity Set
        ps.addPermissionsForEntitySet( ROLE_GOVERNOR,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ImmutableSet.of( Permission.ALTER ) );
        ps.addPermissionsForEntitySet( ROLE_READER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ImmutableSet.of( Permission.READ ) );
        ps.addPermissionsForEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ImmutableSet.of( Permission.WRITE ) );
    }

    private EntitySet entitySetMetadataLookup( User user, FullQualifiedName entityTypeFqn, String entitySetName ) {
        setUser( user );
        EntitySet result = dms.getEntitySet( entityTypeFqn, entitySetName );
        System.out.println( user.getName() + " getting Entity Set metadata for " + entitySetName + " of type "
                + entityTypeFqn + ": " + result );
        return result;
    }

    private EntitySet entitySetMetadataLookup(
            User user,
            FullQualifiedName entityTypeFqn,
            String entitySetName,
            EntitySet expectedResult ) {
        EntitySet result = entitySetMetadataLookup( user, entityTypeFqn, entitySetName );
        if ( expectedResult != null ) {
            Assert.assertEquals( expectedResult, result );
        } else {
            Assert.assertNull( result );
        }
        return result;
    }

    private void entitySetTest() {
        System.out.println( "***Entity Set Test starts***" );
        // Test 1: Citizen is given the DISCOVER permission for Secret Service; and the right got removed after.
        // Expected: RandomGuy can see the metadata for Secret Service; and cannot after.
        EntitySet answer1 = entitySetMetadataLookup( USER_GOD, NATION_CITIZENS, NATION_SECRET_SERVICE );

        ps.addPermissionsForEntitySet( ROLE_CITIZEN,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ImmutableSet.of( Permission.DISCOVER ) );
        entitySetMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, NATION_SECRET_SERVICE, answer1 );

        ps.removePermissionsForEntitySet( ROLE_CITIZEN,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ImmutableSet.of( Permission.DISCOVER ) );
        entitySetMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, NATION_SECRET_SERVICE, null );

        // Test 2: Citizen is given the DISCOVER permission for NATION_SECRET_SERVICE. RandomGuy also got added to the
        // role Reader.
        // The Discover permission of Citizen is removed after.
        // Expected: RandomGuy can still see the metadata for NATION_SECRET_SERVICE.
        EntitySet answer2 = entitySetMetadataLookup( USER_GOD, NATION_CITIZENS, NATION_SECRET_SERVICE );

        ps.addPermissionsForEntitySet( ROLE_CITIZEN,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ImmutableSet.of( Permission.DISCOVER ) );
        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of( Permission.DISCOVER ) );
        USER_RANDOMGUY.addRoles( Arrays.asList( ROLE_READER ) );
        entitySetMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, NATION_SECRET_SERVICE, answer2 );

        ps.removePermissionsForEntitySet( ROLE_CITIZEN,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ImmutableSet.of( Permission.DISCOVER ) );
        entitySetMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, NATION_SECRET_SERVICE, answer2 );

        USER_RANDOMGUY.removeRoles( Arrays.asList( ROLE_READER ) );
    }

    /**
     * WARNING: This test creates garbage data. WARNING: Check tables to see if output is correct.
     */
    private void propertyTypeInEntityTypeTest() {
        System.out.println( "***Property Type In Entity Type Test Starts***" );
        // Setup: User gets the role of Reader, which gives him the Permission to READ NATION_CITIZENS and
        // NATION_SECRET_SERVICE
        // Give Reader READ rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in NATION_CITIZENS
        // User gets the role of Writer, which gives him the Permission to WRITE NATION_CITIZENS and
        // NATION_SECRET_SERVICE
        setUser( USER_RANDOMGUY );
        USER_RANDOMGUY.addRoles( Arrays.asList( ROLE_READER, ROLE_WRITER ) );
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_READER,
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.READ ) );
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_READER,
                NATION_CITIZENS,
                ADDRESS,
                ImmutableSet.of( Permission.READ ) );
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_READER,
                NATION_CITIZENS,
                POSITION,
                ImmutableSet.of( Permission.READ ) );
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_READER,
                NATION_CITIZENS,
                LIFE_EXPECTANCY,
                ImmutableSet.of( Permission.READ ) );

        // Test 1: give Writer WRITE rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in NATION_CITIZENS.
        // Expected: Being able to read and write all data types.

        ps.addPermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                ADDRESS,
                ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                POSITION,
                ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                LIFE_EXPECTANCY,
                ImmutableSet.of( Permission.WRITE ) );

        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                ADDRESS,
                Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                POSITION,
                Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                LIFE_EXPECTANCY,
                Permission.WRITE ) );

        // Test 1.33: Actually write data to entity type
        createData( 10,
                NATION_CITIZENS,
                Optional.absent(),
                ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 1.67: Read data from entity type - should be able to read all data.
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                Permission.READ ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                ADDRESS,
                Permission.READ ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                POSITION,
                Permission.READ ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                LIFE_EXPECTANCY,
                Permission.READ ) );

        System.out.println( " -- READ TEST 1 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result1 = dataService.readAllEntitiesOfType( NATION_CITIZENS );
        for ( Multimap<FullQualifiedName, Object> entity : result1 ) {
            System.out.println( entity );
        }

        // Cleanup: remove WRITE rights for Writer.
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.WRITE ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                ADDRESS,
                ImmutableSet.of( Permission.WRITE ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                POSITION,
                ImmutableSet.of( Permission.WRITE ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                LIFE_EXPECTANCY,
                ImmutableSet.of( Permission.WRITE ) );

        // Test 2: given Writer WRITE rights for EMPLOYEE_ID, ADDRESS in NATION_CITIZENS
        // Expected: Only have WRITE rights for (EMPLOYEE_ID, NATION_CITIZENS), (ADDRESS, NATION_CITIZENS)

        ps.addPermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                ADDRESS,
                ImmutableSet.of( Permission.WRITE ) );

        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                ADDRESS,
                Permission.WRITE ) );
        Assert.assertFalse( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                POSITION,
                Permission.WRITE ) );
        Assert.assertFalse( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                LIFE_EXPECTANCY,
                Permission.WRITE ) );

        // Test 2.33: Actually write data - only the columns EMPLOYEE_ID, ADDRESS would be non-empty
        createData( 10,
                NATION_CITIZENS,
                Optional.absent(),
                ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 2.67: Read data - should be able to read all data, but only EMPLOYEE_ID and ADDRESS are non-null.
        System.out.println( " -- READ TEST 2 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result2 = dataService.readAllEntitiesOfType( NATION_CITIZENS );
        for ( Multimap<FullQualifiedName, Object> entity : result2 ) {
            System.out.println( entity );
        }

        // Cleanup: remove WRITE rights for Writer.
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.WRITE ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                ADDRESS,
                ImmutableSet.of( Permission.WRITE ) );

        // Cleanup: remove Reader/Writer Role, and all current Read rights.
        USER_RANDOMGUY.removeRoles( Arrays.asList( ROLE_READER, ROLE_WRITER ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_READER,
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.READ ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_READER,
                NATION_CITIZENS,
                ADDRESS,
                ImmutableSet.of( Permission.READ ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_READER,
                NATION_CITIZENS,
                POSITION,
                ImmutableSet.of( Permission.READ ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_READER,
                NATION_CITIZENS,
                LIFE_EXPECTANCY,
                ImmutableSet.of( Permission.READ ) );

        // Setup: User gets the role of Reader, which gives him the Permission to READ NATION_CITIZENS and
        // NATION_SECRET_SERVICE
        // Give Reader READ rights for EMPLOYEE_ID, ADDRESS, POSITION in NATION_CITIZENS
        // User gets the role of Writer, which gives him the Permission to WRITE NATION_CITIZENS and
        // NATION_SECRET_SERVICE
        USER_RANDOMGUY.addRoles( Arrays.asList( ROLE_READER, ROLE_WRITER ) );
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_READER,
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.READ ) );
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_READER,
                NATION_CITIZENS,
                ADDRESS,
                ImmutableSet.of( Permission.READ ) );
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_READER,
                NATION_CITIZENS,
                POSITION,
                ImmutableSet.of( Permission.READ ) );

        // Test 3: give Writer WRITE rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in NATION_CITIZENS
        // Expected: Have WRITE rights for all pairs (EMPLOYEE_ID, NATION_CITIZENS),..., (LIFE_EXPECTANCY,
        // NATION_CITIZENS)
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                ADDRESS,
                ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                POSITION,
                ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                LIFE_EXPECTANCY,
                ImmutableSet.of( Permission.WRITE ) );

        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                ADDRESS,
                Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                POSITION,
                Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                LIFE_EXPECTANCY,
                Permission.WRITE ) );
        // Test 3.33: Actually write data - all columns should be written.
        createData( 10,
                NATION_CITIZENS,
                Optional.absent(),
                ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 3.67: Read data - should be unable to read LIFE_EXPECTANCY.
        System.out.println( " -- READ TEST 3 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result3 = dataService.readAllEntitiesOfType( NATION_CITIZENS );
        for ( Multimap<FullQualifiedName, Object> entity : result3 ) {
            System.out.println( entity );
        }

        // Cleanup: remove WRITE rights for Writer.
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.WRITE ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                ADDRESS,
                ImmutableSet.of( Permission.WRITE ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                POSITION,
                ImmutableSet.of( Permission.WRITE ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                LIFE_EXPECTANCY,
                ImmutableSet.of( Permission.WRITE ) );

        // Test 4: given Writer WRITE rights for EMPLOYEE_ID, ADDRESS. Inherit rights from NATION_CITIZENS
        // Expected: Only have WRITE rights for (EMPLOYEE_ID, NATION_CITIZENS), (ADDRESS, NATION_CITIZENS)
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                ADDRESS,
                ImmutableSet.of( Permission.WRITE ) );

        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                ADDRESS,
                Permission.WRITE ) );
        Assert.assertFalse( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                POSITION,
                Permission.WRITE ) );
        Assert.assertFalse( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                LIFE_EXPECTANCY,
                Permission.WRITE ) );

        // Test 4.33: Actually write data - only the columns EMPLOYEE_ID, ADDRESS would be non-empty
        createData( 10,
                NATION_CITIZENS,
                Optional.absent(),
                ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 4.67: Read data - should be able to read (EMPLOYEE_ID, ADDRESS, POSITION), but only EMPLOYEE_ID and
        // ADDRESS are non-null.
        System.out.println( " -- READ TEST 4 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result4 = dataService.readAllEntitiesOfType( NATION_CITIZENS );
        for ( Multimap<FullQualifiedName, Object> entity : result4 ) {
            System.out.println( entity );
        }

        // Cleanup: remove WRITE rights for Citizen.
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.WRITE ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_WRITER,
                NATION_CITIZENS,
                ADDRESS,
                ImmutableSet.of( Permission.WRITE ) );

        // Cleanup: remove Reader/Writer Role, and all current Read rights.
        USER_RANDOMGUY.removeRoles( Arrays.asList( ROLE_READER, ROLE_WRITER ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_READER,
                NATION_CITIZENS,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.READ ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_READER,
                NATION_CITIZENS,
                ADDRESS,
                ImmutableSet.of( Permission.READ ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_READER,
                NATION_CITIZENS,
                POSITION,
                ImmutableSet.of( Permission.READ ) );

    }

    /**
     * WARNING: This test creates garbage data. WARNING: Check tables to see if output is correct.
     */
    private void propertyTypeInEntitySetTest() {
        System.out.println( "***Property Type In Entity Set Test Starts***" );
        // Setup: User gets the role of Reader, which gives him the Permission to READ NATION_CITIZENS and
        // NATION_SECRET_SERVICE
        // Give Reader READ rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in NATION_SECRET_SERVICE.
        // User gets the role of Writer, which gives him the Permission to WRITE NATION_CITIZENS and
        // NATION_SECRET_SERVICE
        setUser( USER_RANDOMGUY );
        USER_RANDOMGUY.addRoles( Arrays.asList( ROLE_READER, ROLE_WRITER ) );
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_READER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.READ ) );
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_READER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                ImmutableSet.of( Permission.READ ) );
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_READER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                POSITION,
                ImmutableSet.of( Permission.READ ) );
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_READER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                LIFE_EXPECTANCY,
                ImmutableSet.of( Permission.READ ) );

        // Test 1: give Writer WRITE rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in
        // NATION_SECRET_SERVICE.
        // Expected: Being able to read and write all data types.

        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                POSITION,
                ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                LIFE_EXPECTANCY,
                ImmutableSet.of( Permission.WRITE ) );

        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                POSITION,
                Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                LIFE_EXPECTANCY,
                Permission.WRITE ) );

        // Test 1.33: Actually write data to entity set
        createData( 10,
                NATION_CITIZENS,
                Optional.of( NATION_SECRET_SERVICE ),
                ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 1.67: Read data from entity set - should be able to read all data.
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                Permission.READ ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                Permission.READ ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                POSITION,
                Permission.READ ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                LIFE_EXPECTANCY,
                Permission.READ ) );

        System.out.println( " -- READ TEST 1 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result1 = dataService.getAllEntitiesOfEntitySet(
                NATION_SECRET_SERVICE, NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName() );
        for ( Multimap<FullQualifiedName, Object> entity : result1 ) {
            System.out.println( entity );
        }

        // Cleanup: remove WRITE rights for Writer.
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.WRITE ) );
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                ImmutableSet.of( Permission.WRITE ) );
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                POSITION,
                ImmutableSet.of( Permission.WRITE ) );
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                LIFE_EXPECTANCY,
                ImmutableSet.of( Permission.WRITE ) );

        // Test 2: given Writer WRITE rights for EMPLOYEE_ID, ADDRESS. Inherit rights from NATION_SECRET_SERVICE
        // Expected: Only have WRITE rights for (EMPLOYEE_ID, NATION_SECRET_SERVICE), (ADDRESS, NATION_SECRET_SERVICE)

        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                ImmutableSet.of( Permission.WRITE ) );

        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                Permission.WRITE ) );
        Assert.assertFalse( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                POSITION,
                Permission.WRITE ) );
        Assert.assertFalse( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                LIFE_EXPECTANCY,
                Permission.WRITE ) );

        // Test 2.33: Actually write data - only the columns EMPLOYEE_ID, ADDRESS would be non-empty
        createData( 10,
                NATION_CITIZENS,
                Optional.of( NATION_SECRET_SERVICE ),
                ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 2.67: Read data - should be able to read all data, but only EMPLOYEE_ID and ADDRESS are non-null.
        System.out.println( " -- READ TEST 2 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result2 = dataService.getAllEntitiesOfEntitySet(
                NATION_SECRET_SERVICE, NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName() );
        for ( Multimap<FullQualifiedName, Object> entity : result2 ) {
            System.out.println( entity );
        }

        // Cleanup: remove WRITE rights for Writer.
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.WRITE ) );
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                ImmutableSet.of( Permission.WRITE ) );

        // Cleanup: remove Reader/Writer Role, and all current Read rights.
        USER_RANDOMGUY.removeRoles( Arrays.asList( ROLE_READER, ROLE_WRITER ) );
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_READER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.READ ) );
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_READER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                ImmutableSet.of( Permission.READ ) );
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_READER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                POSITION,
                ImmutableSet.of( Permission.READ ) );
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_READER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                LIFE_EXPECTANCY,
                ImmutableSet.of( Permission.READ ) );

        // Setup: User gets the role of Reader, which gives him the Permission to READ NATION_CITIZENS and
        // NATION_SECRET_SERVICE
        // Give Reader READ rights for EMPLOYEE_ID, ADDRESS, POSITION in NATION_SECRET_SERVICE
        // User gets the role of Writer, which gives him the Permission to WRITE NATION_CITIZENS and
        // NATION_SECRET_SERVICE
        USER_RANDOMGUY.addRoles( Arrays.asList( ROLE_READER, ROLE_WRITER ) );
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_READER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.READ ) );
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_READER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                ImmutableSet.of( Permission.READ ) );
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_READER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                POSITION,
                ImmutableSet.of( Permission.READ ) );

        // Test 3: give Writer WRITE rights for EMPLOYEE_ID, ADDRESS, POSITION, and LIFE_EXPECTANCY in
        // NATION_SECRET_SERVICE
        // Expected: Have WRITE rights for all pairs (EMPLOYEE_ID, NATION_SECRET_SERVICE),..., (LIFE_EXPECTANCY,
        // NATION_SECRET_SERVICE)
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                POSITION,
                ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                LIFE_EXPECTANCY,
                ImmutableSet.of( Permission.WRITE ) );

        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                POSITION,
                Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                LIFE_EXPECTANCY,
                Permission.WRITE ) );
        // Test 3.33: Actually write data - all columns should be written.
        createData( 10,
                NATION_CITIZENS,
                Optional.of( NATION_SECRET_SERVICE ),
                ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 3.67: Read data - should be unable to read LIFE_EXPECTANCY.
        System.out.println( " -- READ TEST 3 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result3 = dataService.getAllEntitiesOfEntitySet(
                NATION_SECRET_SERVICE, NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName() );
        for ( Multimap<FullQualifiedName, Object> entity : result3 ) {
            System.out.println( entity );
        }

        // Cleanup: remove WRITE rights for Writer.
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.WRITE ) );
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                ImmutableSet.of( Permission.WRITE ) );
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                POSITION,
                ImmutableSet.of( Permission.WRITE ) );
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                LIFE_EXPECTANCY,
                ImmutableSet.of( Permission.WRITE ) );

        // Test 4: given Writer WRITE rights for EMPLOYEE_ID, ADDRESS. Inherit rights from NATION_SECRET_SERVICE
        // Expected: Only have WRITE rights for (EMPLOYEE_ID, NATION_SECRET_SERVICE), (ADDRESS, NATION_SECRET_SERVICE)
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                ImmutableSet.of( Permission.WRITE ) );

        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                Permission.WRITE ) );
        Assert.assertFalse( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                POSITION,
                Permission.WRITE ) );
        Assert.assertFalse( ps.checkUserHasPermissionsOnPropertyTypeInEntitySet( USER_RANDOMGUY.getRoles(),
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                LIFE_EXPECTANCY,
                Permission.WRITE ) );

        // Test 4.33: Actually write data - only the columns EMPLOYEE_ID, ADDRESS would be non-empty
        createData( 10,
                NATION_CITIZENS,
                Optional.of( NATION_SECRET_SERVICE ),
                ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        ADDRESS,
                        POSITION,
                        LIFE_EXPECTANCY ) );

        // Test 4.67: Read data - should be able to read (EMPLOYEE_ID, ADDRESS, POSITION), but only EMPLOYEE_ID and
        // ADDRESS are non-null.
        System.out.println( " -- READ TEST 4 --" );
        Iterable<Multimap<FullQualifiedName, Object>> result4 = dataService.getAllEntitiesOfEntitySet(
                NATION_SECRET_SERVICE, NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName() );
        for ( Multimap<FullQualifiedName, Object> entity : result4 ) {
            System.out.println( entity );
        }

        // Cleanup: remove WRITE rights for Citizen.
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.WRITE ) );
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_WRITER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                ImmutableSet.of( Permission.WRITE ) );

        // Cleanup: remove Reader/Writer Role, and all current Read rights.
        USER_RANDOMGUY.removeRoles( Arrays.asList( ROLE_READER, ROLE_WRITER ) );
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_READER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                ImmutableSet.of( Permission.READ ) );
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_READER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                ADDRESS,
                ImmutableSet.of( Permission.READ ) );
        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_READER,
                NATION_CITIZENS,
                NATION_SECRET_SERVICE,
                POSITION,
                ImmutableSet.of( Permission.READ ) );
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

            entity.put( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ), UUID.randomUUID() );

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

        dataService.createEntityData( createEntityRequest );
    }

}
