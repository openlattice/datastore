package com.kryptnostic.datastore.edm;

import java.util.Collections;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.Constants;
import com.kryptnostic.datastore.Permission;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.EdmService;
import com.kryptnostic.datastore.services.PermissionsService;
import com.kryptnostic.instrumentation.v1.exceptions.types.UnauthorizedException;

public class BabyAclTest extends BootstrapDatastoreWithCassandra {

    protected static final String            ROLE_READER           = "reader";
    protected static final String            ROLE_WRITER           = "writer";
    protected static final String            ROLE_GOVERNOR         = "governor";
    protected static final String            ROLE_CITIZEN          = "citizen";
    protected static final User              USER_RANDOMGUY          = new User(
            "RANDOM_GUY",
            Sets.newHashSet( ROLE_CITIZEN ) );

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
        propertyTypeTest();
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

        dms.deleteEntitySet( NATION_CITIZENS, NATION_SECRET_SERVICE );
        
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
                        new FullQualifiedName( NATION_NAMESPACE, LIFE_EXPECTANCY.getName() ),
                        new FullQualifiedName( NATION_NAMESPACE, ADDRESS.getName() ) ) )
                .setViewableProperties( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        new FullQualifiedName( NATION_NAMESPACE, LIFE_EXPECTANCY.getName() ),
                        new FullQualifiedName( NATION_NAMESPACE, ADDRESS.getName() ) ) );
        dms.createEntityType( citizens );

        // God creates entity set Secret Service
        EntitySet secretService = new EntitySet().setType( NATION_CITIZENS )
                .setName( NATION_SECRET_SERVICE )
                .setTitle( "Every nation would have one");
        dms.createEntitySet( secretService );
    }

    private static void grantRights() {
        // God grants rights to roles
        setUser( USER_GOD );
        // Property Types        
        ps.addPermissionsForPropertyType( ROLE_READER, ADDRESS, ImmutableSet.of( Permission.READ ) );
        ps.addPermissionsForPropertyType( ROLE_READER, POSITION, ImmutableSet.of( Permission.READ ) );

        ps.addPermissionsForPropertyType( ROLE_WRITER, ADDRESS, ImmutableSet.of( Permission.WRITE ) );
        ps.addPermissionsForPropertyType( ROLE_WRITER, POSITION, ImmutableSet.of( Permission.WRITE ) );
        
        //Entity Types 
        ps.addPermissionsForEntityType( ROLE_GOVERNOR, NATION_CITIZENS,
                ImmutableSet.of(Permission.ALTER) );
        ps.addPermissionsForEntityType( ROLE_READER, NATION_CITIZENS,
                ImmutableSet.of(Permission.READ) );
        ps.addPermissionsForEntityType( ROLE_WRITER, NATION_CITIZENS,
                ImmutableSet.of(Permission.WRITE) );
        
        //Entity Set
        ps.addPermissionsForEntitySet( ROLE_GOVERNOR, NATION_CITIZENS, NATION_SECRET_SERVICE,
                ImmutableSet.of(Permission.ALTER) );
        ps.addPermissionsForEntitySet( ROLE_READER, NATION_CITIZENS, NATION_SECRET_SERVICE,
                ImmutableSet.of(Permission.READ) );
        ps.addPermissionsForEntitySet( ROLE_WRITER, NATION_CITIZENS, NATION_SECRET_SERVICE,
                ImmutableSet.of(Permission.WRITE) );        
    }

    private PropertyType propertyTypeMetadataLookup( User user, FullQualifiedName propertyTypeFqn ) {
        setUser( user );
        PropertyType result = dms.getPropertyType( propertyTypeFqn );
        System.out.println( user.getName() + " getting Property Types metadata for " + propertyTypeFqn + ": " + result );
        return result;
    }

    private PropertyType propertyTypeMetadataLookup(
            User user,
            FullQualifiedName propertyTypeFqn,
            PropertyType expectedResult ) {
        PropertyType result = propertyTypeMetadataLookup( user, propertyTypeFqn );
        if ( expectedResult != null ) {
            Assert.assertEquals( expectedResult, result );
        } else {
            Assert.assertNull( result );
        }
        return result;
    }

    private void propertyTypeTest() {
        System.out.println( "***Property Type Test starts***" );
        // Test 1: Citizen does not have the DISCOVER permission for Address
        // Expected: RandomGuy cannot see the metadata for address
        propertyTypeMetadataLookup( USER_RANDOMGUY, ADDRESS, null );

        // Test 2: Citizen is given the DISCOVER permission for Address; and the right got removed after.
        // Expected: RandomGuy can see the metadata for address; and cannot after.
        PropertyType answer2 = propertyTypeMetadataLookup( USER_GOD, ADDRESS );
        ps.addPermissionsForPropertyType( ROLE_CITIZEN, ADDRESS, ImmutableSet.of(Permission.DISCOVER) );
        propertyTypeMetadataLookup( USER_RANDOMGUY, ADDRESS, answer2 );

        ps.removePermissionsForPropertyType( ROLE_CITIZEN, ADDRESS, ImmutableSet.of(Permission.DISCOVER) );
        propertyTypeMetadataLookup( USER_RANDOMGUY, ADDRESS, null );
        
        // Test 3: Citizen is given the DISCOVER permission for Address. RandomGuy also got added to the role Reader.
        // The Discover permission of Citizen is removed after.
        // Expected: RandomGuy can still see the metadata for address.
        PropertyType answer3 = propertyTypeMetadataLookup( USER_GOD, ADDRESS );
        ps.addPermissionsForPropertyType( ROLE_CITIZEN, ADDRESS, ImmutableSet.of(Permission.DISCOVER) );
        USER_RANDOMGUY.addRoles( ImmutableSet.of(ROLE_READER) );
        propertyTypeMetadataLookup( USER_RANDOMGUY, ADDRESS, answer3 );
        
        ps.removePermissionsForPropertyType( ROLE_CITIZEN, ADDRESS, ImmutableSet.of(Permission.DISCOVER) );
        propertyTypeMetadataLookup( USER_RANDOMGUY, ADDRESS, answer3 );
        USER_RANDOMGUY.removeRoles( ImmutableSet.of(ROLE_READER) );
        
        // Test 4: RandomGuy creates property SPIED_ON. Current setting is everyone can create type. 
        // Expected: RandomGuy CANNOT access the property he just created; this is intended, access rights should come from Admin
        // Test: Citizens receive the right to Discover SPIED_ON
        // Expected: Now RandomGuy can discover SPIED_ON
        setUser( USER_RANDOMGUY );
        PropertyType spiedOn = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( SPIED_ON.getName() )
                .setDatatype( EdmPrimitiveTypeKind.Boolean ).setMultiplicity( 0 );
        dms.createPropertyType( spiedOn );
        propertyTypeMetadataLookup( USER_RANDOMGUY, SPIED_ON, null);
        ps.addPermissionsForPropertyType( ROLE_CITIZEN, SPIED_ON, ImmutableSet.of(Permission.DISCOVER) );
        
        propertyTypeMetadataLookup( USER_RANDOMGUY, SPIED_ON, spiedOn);
        ps.removePermissionsForPropertyType( ROLE_CITIZEN, SPIED_ON, ImmutableSet.of(Permission.DISCOVER) );
    }

    private EntityType entityTypeMetadataLookup( User user, FullQualifiedName entityTypeFqn ) {
        setUser( user );
        EntityType result = dms.getEntityType( entityTypeFqn );
        System.out.println( user.getName() + " getting Entity Types metadata for " + entityTypeFqn + ": " + result );
        return result;
    }
    
    private EntityType entityTypeMetadataLookup(
            User user,
            FullQualifiedName entityTypeFqn,
            EntityType expectedResult ) {
        EntityType result = entityTypeMetadataLookup( user, entityTypeFqn );
        if ( expectedResult != null ) {
            Assert.assertEquals( expectedResult, result );
        } else {
            Assert.assertNull( result );
        }
        return result;
    }

    private void entityTypeTest() {
        System.out.println( "***Entity Type Test starts***" );
        // Test 1: Citizen does not have the DISCOVER permission for NATION_CITIZENS
        // Expected: RandomGuy cannot see the metadata for NATION_CITIZENS
        entityTypeMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, null );

        // Test 2: Citizen is given the DISCOVER permission for NATION_CITIZENS; and the right got removed after.
        // Expected: RandomGuy can see the metadata for NATION_CITIZENS; and cannot after.
        EntityType answer2 = entityTypeMetadataLookup( USER_GOD, NATION_CITIZENS );
        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );
        entityTypeMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, answer2 );

        ps.removePermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );
        entityTypeMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, null );
        
        // Test 3: Citizen is given the DISCOVER permission for NATION_CITIZENS. RandomGuy also got added to the role Reader.
        // The Discover permission of Citizen is removed after.
        // Expected: RandomGuy can still see the metadata for NATION_CITIZENS.
        EntityType answer3 = entityTypeMetadataLookup( USER_GOD, NATION_CITIZENS );
        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );
        USER_RANDOMGUY.addRoles( ImmutableSet.of(ROLE_READER) );
        entityTypeMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, answer3 );
        
        ps.removePermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );
        entityTypeMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, answer3 );
        USER_RANDOMGUY.removeRoles( ImmutableSet.of(ROLE_READER) );
        
        // Test 4: Citizen has ALTER permission for NATION_CITIZENS
        // Expected: RandomGuy can add/remove property types from NATION_CITIZENS.
        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.ALTER) );
        setUser( USER_RANDOMGUY );
        dms.addPropertyTypesToEntityType( NATION_CITIZENS.getNamespace(),
                NATION_CITIZENS.getName(),
                ImmutableSet.of( SPIED_ON ) );
        dms.removePropertyTypesFromEntityType( NATION_CITIZENS.getNamespace(),
                NATION_CITIZENS.getName(),
                ImmutableSet.of( SPIED_ON ) );
        ps.removePermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.ALTER) );

        // Test 5: Citizen has ALTER permission for NATION_CITIZENS
        // Expected: RandomGuy can delete the entity type NATION_CITIZENS.
        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.ALTER) );
        setUser( USER_RANDOMGUY );
        dms.deleteEntityType( NATION_CITIZENS );
        
        EntityType citizens = new EntityType().setNamespace( NATION_NAMESPACE ).setName( NATION_CITIZENS.getName() )
                .setKey( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ) ) )
                .setProperties( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        new FullQualifiedName( NATION_NAMESPACE, LIFE_EXPECTANCY.getName() ),
                        new FullQualifiedName( NATION_NAMESPACE, ADDRESS.getName() ) ) )
                .setViewableProperties( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        new FullQualifiedName( NATION_NAMESPACE, LIFE_EXPECTANCY.getName() ),
                        new FullQualifiedName( NATION_NAMESPACE, ADDRESS.getName() ) ) );
        dms.createEntityType( citizens );
        ps.removePermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.ALTER) );
    }

    private EntitySet entitySetMetadataLookup( User user, FullQualifiedName entityTypeFqn, String entitySetName ) {
        setUser( user );
        EntitySet result = dms.getEntitySet( entityTypeFqn, entitySetName );
        System.out.println( user.getName() + " getting Entity Set metadata for " + entitySetName + " of type " + entityTypeFqn + ": " + result );
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
        // Test 1: Citizen does not have the DISCOVER permission for NATION_CITIZENS
        // Expected: RandomGuy cannot see the metadata for any entity sets in NATION_CITIZENS
        entitySetMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, NATION_SECRET_SERVICE, null );
        
        // Test 2: Citizen is given the DISCOVER permission for NATION_CITIZENS, but does not have the DISCOVER permission for Secret Service in NATION_CITIZENS
        // Expected: RandomGuy cannot see the metadata for Secret Service in NATION_CITIZENS
        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );
        entitySetMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, NATION_SECRET_SERVICE, null );
        ps.removePermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );

        // Test 3: Citizen is given the DISCOVER permission for Secret Service; and the right got removed after.
        // Expected: RandomGuy can see the metadata for Secret Service; and cannot after.
        EntitySet answer3 = entitySetMetadataLookup( USER_GOD, NATION_CITIZENS, NATION_SECRET_SERVICE );
        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );
        ps.addPermissionsForEntitySet( ROLE_CITIZEN, NATION_CITIZENS, NATION_SECRET_SERVICE, ImmutableSet.of(Permission.DISCOVER) );
        entitySetMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, NATION_SECRET_SERVICE, answer3 );
        
        ps.removePermissionsForEntitySet( ROLE_CITIZEN, NATION_CITIZENS, NATION_SECRET_SERVICE, ImmutableSet.of(Permission.DISCOVER) );
        entitySetMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, NATION_SECRET_SERVICE, null );
        
        ps.removePermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );

        // Test 4: Citizen is given the DISCOVER permission for NATION_CITIZENS. RandomGuy also got added to the role Reader.
        // The Discover permission of Citizen is removed after.
        // Expected: RandomGuy can still see the metadata for NATION_CITIZENS.
        EntitySet answer4 = entitySetMetadataLookup( USER_GOD, NATION_CITIZENS, NATION_SECRET_SERVICE );
        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );
        ps.addPermissionsForEntitySet( ROLE_CITIZEN, NATION_CITIZENS, NATION_SECRET_SERVICE, ImmutableSet.of(Permission.DISCOVER) );        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );
        USER_RANDOMGUY.addRoles( ImmutableSet.of(ROLE_READER) );
        entitySetMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, NATION_SECRET_SERVICE, answer4 );
        
        ps.removePermissionsForEntitySet( ROLE_CITIZEN, NATION_CITIZENS, NATION_SECRET_SERVICE, ImmutableSet.of(Permission.DISCOVER) );
        ps.removePermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );
        entitySetMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, NATION_SECRET_SERVICE, answer4 );
        USER_RANDOMGUY.removeRoles( ImmutableSet.of(ROLE_READER) );
    }
    
    private void propertyTypeInEntityTypeTest() {
        System.out.println( "***Property Type In Entity Type Test Starts***" );
        //Setup: 
        //SPIED_ON (property type) is added to NATION_CITIZENS (entity type)
        //Citizen has no rights to anything
        setUser(USER_GOD);
        dms.addPropertyTypesToEntityType( NATION_CITIZENS.getNamespace(),
                NATION_CITIZENS.getName(),
                ImmutableSet.of( SPIED_ON ) );
        
        // Test 1: Citizen has Discover permission to both SPIED_ON (property type) and NATION_CITIZENS (entity type), but not the pair
        // Expected: RandomGuy does not see SPIED_ON in NATION_CITIZENS
        ps.addPermissionsForPropertyType( ROLE_CITIZEN, SPIED_ON, ImmutableSet.of(Permission.DISCOVER) );
        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );
        
        propertyTypeMetadataLookup( USER_RANDOMGUY, SPIED_ON );
        entityTypeMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS );
        
        ps.removePermissionsForPropertyType( ROLE_CITIZEN, SPIED_ON, ImmutableSet.of(Permission.DISCOVER) );
        ps.removePermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );     
        
        // Test 2: Citizen is given the Discover permission to the pair (SPIED_ON, NATION_CITIZENS)
        // Expected: RandomGuy can see SPIED_ON in NATION_CITIZENS
        ps.addPermissionsForPropertyType( ROLE_CITIZEN, SPIED_ON, ImmutableSet.of(Permission.DISCOVER) );
        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );
        ps.addPermissionsForPropertyTypeInEntityType( ROLE_CITIZEN, NATION_CITIZENS, SPIED_ON, ImmutableSet.of(Permission.DISCOVER) );

        entityTypeMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS );
        
        ps.removePermissionsForPropertyType( ROLE_CITIZEN, SPIED_ON, ImmutableSet.of(Permission.DISCOVER) );
        ps.removePermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_CITIZEN, NATION_CITIZENS, SPIED_ON, ImmutableSet.of(Permission.DISCOVER) );
     
        // INHERITED RIGHTS TEST
        // Test 3: Citizen has Read permission for NATION_CITIZENS and SPIED_ON; Inheritance from Both Rights
        // Expected: RandomGuy has Read permission for the pair (NATION_CITIZENS, SPIED_ON)
        ps.addPermissionsForPropertyType( ROLE_CITIZEN, SPIED_ON, ImmutableSet.of(Permission.READ) );
        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.READ) );
        
        ps.inheritPermissionsFromBothTypes( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(SPIED_ON) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( ImmutableSet.of(ROLE_CITIZEN), NATION_CITIZENS, SPIED_ON, Permission.READ ) );
        
        ps.removePermissionsForPropertyType( ROLE_CITIZEN, SPIED_ON, ImmutableSet.of(Permission.READ) );
        ps.removePermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.READ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_CITIZEN, NATION_CITIZENS, SPIED_ON, ImmutableSet.of(Permission.READ) );

        // Test 4: Citizen has Read permission NATION_CITIZENS and DISCOVER permission for SPIED_ON; Inheritance from Both Rights
        // Expected: RandomGuy has Discover permission for the pair (NATION_CITIZENS, SPIED_ON)
        ps.addPermissionsForPropertyType( ROLE_CITIZEN, SPIED_ON, ImmutableSet.of(Permission.DISCOVER) );
        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.READ) );
        
        ps.inheritPermissionsFromBothTypes( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(SPIED_ON) );
        Assert.assertFalse( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( ImmutableSet.of(ROLE_CITIZEN), NATION_CITIZENS, SPIED_ON, Permission.READ ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( ImmutableSet.of(ROLE_CITIZEN), NATION_CITIZENS, SPIED_ON, Permission.DISCOVER ) );
        
        ps.removePermissionsForPropertyType( ROLE_CITIZEN, SPIED_ON, ImmutableSet.of(Permission.DISCOVER) );
        ps.removePermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.READ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_CITIZEN, NATION_CITIZENS, SPIED_ON, ImmutableSet.of(Permission.DISCOVER) );

        // Test 5: Citizen has DISCOVER permission NATION_CITIZENS and READ permission for SPIED_ON; Inheritance from Both Rights
        // Expected: RandomGuy has Discover permission for the pair (NATION_CITIZENS, SPIED_ON)
        ps.addPermissionsForPropertyType( ROLE_CITIZEN, SPIED_ON, ImmutableSet.of(Permission.READ) );
        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );
        
        ps.inheritPermissionsFromBothTypes( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(SPIED_ON) );
        Assert.assertFalse( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( ImmutableSet.of(ROLE_CITIZEN), NATION_CITIZENS, SPIED_ON, Permission.READ ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( ImmutableSet.of(ROLE_CITIZEN), NATION_CITIZENS, SPIED_ON, Permission.DISCOVER ) );
        
        ps.removePermissionsForPropertyType( ROLE_CITIZEN, SPIED_ON, ImmutableSet.of(Permission.READ) );
        ps.removePermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_CITIZEN, NATION_CITIZENS, SPIED_ON, ImmutableSet.of(Permission.DISCOVER) );

        /**
         * Citizen has WRITE Permission for Property Type POSITION.
         * POSITION gets added to NATION_CITIZENS
         */
        ps.addPermissionsForPropertyType( ROLE_CITIZEN, POSITION, ImmutableSet.of(Permission.WRITE) );
        dms.addPropertyTypesToEntityType( NATION_CITIZENS.getNamespace(),
                NATION_CITIZENS.getName(),
                ImmutableSet.of( POSITION ) );
        // Test 6: Inheritance from Property Rights.
        // Expected: RandomGuy has WRITE permission on NATION_CITIZENS, and (NATION_CITIZENS, POSITION)        
        ps.inheritPermissionsFromPropertyType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(POSITION) );

        Assert.assertTrue( ps.checkUserHasPermissionsOnEntityType( ImmutableSet.of(ROLE_CITIZEN), NATION_CITIZENS, Permission.WRITE ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( ImmutableSet.of(ROLE_CITIZEN), NATION_CITIZENS, POSITION, Permission.WRITE ) );
        
        ps.removePermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.WRITE) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_CITIZEN, NATION_CITIZENS, POSITION, ImmutableSet.of(Permission.WRITE) );

        // Test 7: Citizen being given READ right for Property Type for LIFE_EXPECTANCY. Inheritance from Property Rights.
        // Expected: RandomGuy has READ permission on NATION_CITIZENS, and (NATION_CITIZENS, LIFE_EXPECTANCY)
        ps.addPermissionsForPropertyType( ROLE_CITIZEN, LIFE_EXPECTANCY, ImmutableSet.of(Permission.READ) );
        ps.inheritPermissionsFromPropertyType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(LIFE_EXPECTANCY) );

        Assert.assertTrue( ps.checkUserHasPermissionsOnEntityType( ImmutableSet.of(ROLE_CITIZEN), NATION_CITIZENS, Permission.READ ) );
        Assert.assertTrue( ps.checkUserHasPermissionsOnPropertyTypeInEntityType( ImmutableSet.of(ROLE_CITIZEN), NATION_CITIZENS, LIFE_EXPECTANCY, Permission.READ ) );
        
        ps.removePermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.READ) );
        ps.removePermissionsForPropertyTypeInEntityType( ROLE_CITIZEN, NATION_CITIZENS, LIFE_EXPECTANCY, ImmutableSet.of(Permission.READ) );
        
        /** 
         * READ and WRITE Test once DataApi is done
         */
        // Test 5: Citizen got all permissions removed. Citizen has Read permission for SPIED_ON, and Discover permission for NATION_CITIZENS
        // Expected: RandomGuy cannot perform getEntitiesOfAllType. But he can see SPIED_ON when he asks for metadata for NATION_CITIZENS
                
        // Test 7: Citizen has Write permission for NATION_CITIZENS and all its associated property types
        // Expected: RandomGuy can do createEntityData with all property types
        
        // Test 8: Citizen has Write permission for NATION_CITIZENS and ADDRESS only.
        // Expected: RandomGuy can do createEntityData, but only the property types NATION_CITIZENS and ADDRESS
        
    }

    private void propertyTypeInEntitySetTest() {
        System.out.println( "***Property Type In Entity Set Test Starts***" );        
        // Setup: Citizen has Discover permission to ADDRESS (property type), NATION_CITIZENS (entity type), but not the pair
        ps.addPermissionsForPropertyType( ROLE_CITIZEN, ADDRESS, ImmutableSet.of(Permission.DISCOVER) );
        ps.addPermissionsForEntityType( ROLE_CITIZEN, NATION_CITIZENS, ImmutableSet.of(Permission.DISCOVER) );

        propertyTypeMetadataLookup( USER_RANDOMGUY, SPIED_ON );
        entityTypeMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS );
        // Test 1: Citizen has Discover permission to NATION_SECRET_SERVICE (entity set), but not the pair (ADDRESS, NATION_SECRET_SEVICE)
        // Expected: RandomGuy does not see ADDRESS in NATION_SECRET_SERVICE
        ps.addPermissionsForEntitySet( ROLE_CITIZEN, NATION_CITIZENS, NATION_SECRET_SERVICE, ImmutableSet.of(Permission.DISCOVER) );
        
        entitySetMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, NATION_SECRET_SERVICE );

        ps.removePermissionsForEntitySet( ROLE_CITIZEN, NATION_CITIZENS, NATION_SECRET_SERVICE, ImmutableSet.of(Permission.DISCOVER) );

        // Test 2: Citizen is given the Discover permission to the pair (ADDRESS, NATION_SECRET_SERVICE)
        // Expected: RandomGuy can see ADDRESS in NATION_SECRET_SERVICE
        ps.addPermissionsForEntitySet( ROLE_CITIZEN, NATION_CITIZENS, NATION_SECRET_SERVICE, ImmutableSet.of(Permission.DISCOVER) );
        ps.addPermissionsForPropertyTypeInEntitySet( ROLE_CITIZEN, NATION_CITIZENS, NATION_SECRET_SERVICE, ADDRESS, ImmutableSet.of(Permission.DISCOVER) );
        
        entitySetMetadataLookup( USER_RANDOMGUY, NATION_CITIZENS, NATION_SECRET_SERVICE );

        ps.removePermissionsForPropertyTypeInEntitySet( ROLE_CITIZEN, NATION_CITIZENS, NATION_SECRET_SERVICE, ADDRESS, ImmutableSet.of(Permission.DISCOVER) );
        ps.removePermissionsForEntitySet( ROLE_CITIZEN, NATION_CITIZENS, NATION_SECRET_SERVICE, ImmutableSet.of(Permission.DISCOVER) );
        
        // Write inherited rights test
    }
}
