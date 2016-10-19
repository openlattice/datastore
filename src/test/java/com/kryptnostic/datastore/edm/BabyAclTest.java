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
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.Permission;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.EdmService;
import com.kryptnostic.datastore.services.PermissionsService;
import com.kryptnostic.instrumentation.v1.exceptions.types.UnauthorizedException;

public class BabyAclTest extends BootstrapDatastoreWithCassandra {	
	protected static final String                 USER_PRESIDENT         = "President";
	protected static final String                 USER_CITIZEN           = "Citizen";
	
	protected static final String                 NATION_NAMESPACE       = "us";
	protected static final FullQualifiedName      NATION_SCHEMA          = new FullQualifiedName( NATION_NAMESPACE, "schema" ); //schema
	
	protected static final FullQualifiedName      NATION_CITIZENS        = new FullQualifiedName( NATION_NAMESPACE, "citizens" ); // entity type
	protected static final String                 NATION_SECRET_SERVICE  = "secret-service"; //entity set name
	
	protected static final FullQualifiedName      LIFE_EXPECTANCY        = new FullQualifiedName( NATION_NAMESPACE, "life-expectancy" ); //property type
	protected static final FullQualifiedName      ADDRESS                = new FullQualifiedName( NATION_NAMESPACE, "address" ); //property type
	protected static final FullQualifiedName      POSITION               = new FullQualifiedName( NATION_NAMESPACE, "position" ); //property type
	
	protected static final FullQualifiedName      SPIED_ON               = new FullQualifiedName( NATION_NAMESPACE, "spied-on" ); //property type

	/**
	 * Start of JUnit Tests
	 */
	// Create property types/entity types/entity sets/schemas with different acl's, and modify existing types to change acl's
	// Schema should only show those the user has rights to see.
	@BeforeClass
	public static void initializeAcl(){
		//You are God right now
		setIdentity( USER_GOD );
		addUsers();
		createTypes();
		grantRights();
	}
	
	@Test	
	public void Test(){
		System.out.println("Test starts");
		accessTypes();
		modifyTypes();
	}
	
	// Delete created property types/entity types/entity sets/schemas - Acl tables should update correspondingly.
	@AfterClass
	public static void resetAcl(){
		//God deletes the nation
		//Property Types
		dms.deletePropertyType( LIFE_EXPECTANCY );
		dms.deletePropertyType( ADDRESS );
		dms.deletePropertyType( POSITION );
		dms.deletePropertyType( SPIED_ON );
		
		dms.deleteEntityType( NATION_CITIZENS );
	}
	
	private static void addUsers(){
		uuidForUser.put( USER_PRESIDENT, UUID.randomUUID() );
		uuidForUser.put( USER_CITIZEN, UUID.randomUUID() );
	}
	
	private static void createTypes() {
		//God creates property types Life expectancy, Address
		PropertyType lifeExpectancy = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( LIFE_EXPECTANCY.getName() )
				.setDatatype( EdmPrimitiveTypeKind.Int32).setMultiplicity( 0 );
		PropertyType address = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( ADDRESS.getName() )
				.setDatatype( EdmPrimitiveTypeKind.String).setMultiplicity( 0 );
		PropertyType position = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( POSITION.getName() )
				.setDatatype( EdmPrimitiveTypeKind.String).setMultiplicity( 0 );
		dms.createPropertyType( lifeExpectancy );
		dms.createPropertyType( address );
		dms.createPropertyType( position );
		
		//God creates entity type Citizen, creates entity set President, schema
        EntityType citizens = new EntityType().setNamespace( NATION_NAMESPACE ).setName( NATION_CITIZENS.getName() )
                .setKey( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ) ) )
                .setProperties( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        new FullQualifiedName( NATION_NAMESPACE, LIFE_EXPECTANCY.getName() ),
                        new FullQualifiedName( NATION_NAMESPACE, ADDRESS.getName() ),
                        new FullQualifiedName( NATION_NAMESPACE, POSITION.getName() ) ) );
        dms.createEntityType( citizens );	
	}
	
	private static void grantRights(){
		//God grants rights to others
		//Property Types
		ps.addPermissionsForPropertyType( uuidForUser.get(USER_PRESIDENT), ADDRESS, ImmutableSet.of(Permission.READ, Permission.WRITE) );
		ps.addPermissionsForPropertyType( uuidForUser.get(USER_PRESIDENT), POSITION, ImmutableSet.of(Permission.READ, Permission.WRITE) );
		//Entity Types
		ps.addPermissionsForEntityType( uuidForUser.get(USER_PRESIDENT), NATION_CITIZENS, ImmutableSet.of(Permission.OWNER) );
		ps.addPermissionsForEntityType( uuidForUser.get(USER_CITIZEN), NATION_CITIZENS, ImmutableSet.of(Permission.READ, Permission.WRITE) );
	}

	private void accessTypes(){
		System.out.println("Testing access for various types");
		typeLookup();
	};

	private void modifyTypes(){
		System.out.println("Tesing modification for various types");
		//God allows citizens/president to read Life Expectancy, and allows president to write Life Expectancy
		godGivesRights();
		//President adds property type SPIED_ON, that not even God can access 
		presidentIsWatchingYou();
		//God removes President's permissions after President lets God know about SPIED_ON
		godRemovesRights();
	}
	
	// Look up individual types given Acl
	private void typeLookup(){
		System.out.println("Testing access for property types");
		//God, President, Citizen make get requests for property types/entity types/entity sets/schemas
		propertyTypeMetadataLookup();
		System.out.println("Testing access for entity types");
		entityTypeMetadataLookup();
	};
	
	private PropertyType propertyTypeMetadataLookup( String username, FullQualifiedName propertyTypeFqn){
	    setIdentity( username );
	    PropertyType result = dms.getPropertyType( propertyTypeFqn);
		System.out.println( username + " getting Property Types metadata for " + propertyTypeFqn + ":");
		System.out.println( result );
		return result;
	}
	
	private PropertyType propertyTypeMetadataLookup( String username, FullQualifiedName propertyTypeFqn, PropertyType expectedResult){
		PropertyType result = propertyTypeMetadataLookup( username, propertyTypeFqn );
		if( expectedResult != null){
			Assert.assertEquals( expectedResult, result);
		} else {
			Assert.assertNull( result );
		}
		return result;
	}
	
	private void propertyTypeMetadataLookup() {		
		PropertyType resultAddressForGod = propertyTypeMetadataLookup( USER_GOD, ADDRESS );
		propertyTypeMetadataLookup( USER_PRESIDENT, ADDRESS, resultAddressForGod );
		propertyTypeMetadataLookup( USER_CITIZEN, ADDRESS, null );

		PropertyType resultLifeExpectancyForGod = propertyTypeMetadataLookup( USER_GOD, LIFE_EXPECTANCY );
		propertyTypeMetadataLookup( USER_PRESIDENT, LIFE_EXPECTANCY, resultLifeExpectancyForGod );
		propertyTypeMetadataLookup( USER_CITIZEN, LIFE_EXPECTANCY, null );
	}
	
	private EntityType entityTypeMetadataLookup( String username, FullQualifiedName entityTypeFqn){
	    setIdentity( username );
	    EntityType result = dms.getEntityType( entityTypeFqn);
		System.out.println( username + " getting Entity Types metadata for " + entityTypeFqn + ":");
		System.out.println( result );
		return result;
	}
	
	private EntityType entityTypeMetadataLookup( String username, FullQualifiedName entityTypeFqn, PropertyType expectedResult){
		EntityType result = entityTypeMetadataLookup( username, entityTypeFqn );
		if( expectedResult != null){
			Assert.assertEquals( expectedResult, result);
		} else {
			Assert.assertNull( result );
		}
		return result;
	}
	
	private void entityTypeMetadataLookup() {
		entityTypeMetadataLookup( USER_GOD, NATION_CITIZENS );
		entityTypeMetadataLookup( USER_PRESIDENT, NATION_CITIZENS );
		entityTypeMetadataLookup( USER_CITIZEN, NATION_CITIZENS );
	}
	
	private void godGivesRights(){
		System.out.println("God Gives Rights test starts");
		//God allows Citizen to read LIFE_EXPECTANCY.
		yourFateIsKnown();
		//God allows President to write LIFE_EXPECTANCY.
		//TODO: skipped for now, since modifying values is not implemented in backend yet
//		longLivePresident();
	}
	
	private void yourFateIsKnown(){
		System.out.println("Your Fate Is Known test starts");
		setIdentity( USER_GOD );
		ps.addPermissionsForPropertyTypeInEntityType( uuidForUser.get(USER_PRESIDENT), LIFE_EXPECTANCY, NATION_CITIZENS, ImmutableSet.of(Permission.READ) );
		ps.addPermissionsForPropertyTypeInEntityType( uuidForUser.get(USER_CITIZEN), LIFE_EXPECTANCY, NATION_CITIZENS, ImmutableSet.of(Permission.READ) );

		entityTypeMetadataLookup( USER_GOD, NATION_CITIZENS);
		entityTypeMetadataLookup( USER_PRESIDENT, NATION_CITIZENS);
		entityTypeMetadataLookup( USER_CITIZEN, NATION_CITIZENS);
		/**
		Iterable<Multimap<FullQualifiedName, Object>> resultPresident = aclDs.getAllEntitiesOfType( uuidForUser.get(USER_PRESIDENT), NATION_CITIZENS);
		Iterable<Multimap<FullQualifiedName, Object>> resultCitizen = aclDs.getAllEntitiesOfType( uuidForUser.get(USER_CITIZEN), NATION_CITIZENS);
		
		//write AssertEquals later
		for( Multimap<FullQualifiedName, Object> result: resultPresident ){
			System.out.println( result );
		}
		for( Multimap<FullQualifiedName, Object> result: resultCitizen ){
			System.out.println( result );
		}
		*/
	}
	
	private void presidentIsWatchingYou(){
		System.out.println("President is watching you test starts");
		//President adds SPIED_ON property
		setIdentity( USER_PRESIDENT );
		PropertyType spiedOn = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( SPIED_ON.getName() )
				.setDatatype( EdmPrimitiveTypeKind.Boolean).setMultiplicity( 0 );
		
		dms.createPropertyType( spiedOn );
		dms.addPropertyTypesToEntityType(NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName(), ImmutableSet.of( SPIED_ON ) );
		
		//TODO check everyone's read right on SPIED_ON; perhaps make god try to remove the type
	}
	
	private void godRemovesRights(){	
		System.out.println("God removes rights test starts");
		//God removes all of President's rights except reading Schema. In particular, he should not be able to read/write any types.
		//TODO: if one cannot access an entity type, he shouldn't be able to access the entity sets under it either.
		setIdentity( USER_GOD );
		ps.setPermissionsForPropertyType( uuidForUser.get(USER_PRESIDENT), ADDRESS, Collections.emptySet() );
		ps.setPermissionsForPropertyType( uuidForUser.get(USER_PRESIDENT), POSITION, Collections.emptySet() );
		ps.setPermissionsForPropertyType( uuidForUser.get(USER_PRESIDENT), LIFE_EXPECTANCY, Collections.emptySet() );
		ps.setPermissionsForPropertyType( uuidForUser.get(USER_PRESIDENT), SPIED_ON, Collections.emptySet() );
		ps.setPermissionsForEntityType( uuidForUser.get(USER_PRESIDENT), NATION_CITIZENS, Collections.emptySet() );
		
		//Check what president can see
		propertyTypeMetadataLookup( USER_PRESIDENT, ADDRESS, null );
		propertyTypeMetadataLookup( USER_PRESIDENT, POSITION, null );
		propertyTypeMetadataLookup( USER_PRESIDENT, LIFE_EXPECTANCY, null );
		entityTypeMetadataLookup( USER_PRESIDENT, NATION_CITIZENS);
		
		//God does NOT own the SPIED_ON type, so President should still get access
		PropertyType spiedOn = propertyTypeMetadataLookup( USER_GOD, SPIED_ON );
		propertyTypeMetadataLookup( USER_PRESIDENT, SPIED_ON, spiedOn);
		
		//President surrenders: gives rights of SPIED_ON to God
		setIdentity( USER_PRESIDENT );
		ps.setPermissionsForPropertyType( uuidForUser.get(USER_GOD), SPIED_ON, ImmutableSet.of(Permission.OWNER) );
		//God finishes him off
		setIdentity( USER_GOD );
		ps.setPermissionsForPropertyType( uuidForUser.get(USER_PRESIDENT), SPIED_ON, Collections.emptySet() );
	    //Double check to make sure
		propertyTypeMetadataLookup( USER_PRESIDENT, SPIED_ON, null);
	}	
}
