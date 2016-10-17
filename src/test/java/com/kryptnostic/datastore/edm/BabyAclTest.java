package com.kryptnostic.datastore.edm;

import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
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
	
	protected static final UUID                   PRESIDENT_UUID         = UUID.randomUUID();
	protected static final UUID                   CITIZEN_UUID           = UUID.randomUUID();	

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
		createTypes();
		grantRights();
	}
	
	@Test
	public void accessTypes(){
		typeLookup();
	};
	
	// Delete created property types/entity types/entity sets/schemas - Acl tables should update correspondingly.
	@AfterClass
	public static void resetAcl(){
		//God deletes the nation
		//Property Types
		dms.deletePropertyType( LIFE_EXPECTANCY );
		dms.deletePropertyType( ADDRESS );
		dms.deletePropertyType( POSITION );
	}
	
	private static void createTypes() {
		//You are God right now
		setIdentity( GOD_UUID );
		//God create property types Life expectancy, Address
		PropertyType lifeExpectancy = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( LIFE_EXPECTANCY.getName() )
				.setDatatype( EdmPrimitiveTypeKind.Int32).setMultiplicity( 0 ).setSchemas( ImmutableSet.of() );
		PropertyType address = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( ADDRESS.getName() )
				.setDatatype( EdmPrimitiveTypeKind.String).setMultiplicity( 0 ).setSchemas( ImmutableSet.of() );
		PropertyType position = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( POSITION.getName() )
				.setDatatype( EdmPrimitiveTypeKind.String).setMultiplicity( 0 ).setSchemas( ImmutableSet.of() );
		dms.createPropertyType( lifeExpectancy );
		dms.createPropertyType( address );
		dms.createPropertyType( position );
	}
	
	private static void grantRights(){
		//God grants rights to others
		//Property Types
		ps.addPermissionsForPropertyTypes( PRESIDENT_UUID, ADDRESS, ImmutableSet.of(Permission.READ, Permission.WRITE) );
		ps.addPermissionsForPropertyTypes( PRESIDENT_UUID, POSITION, ImmutableSet.of(Permission.READ, Permission.WRITE) );
	}

	// Look up individual types given Acl
	private void typeLookup(){
		//God, President, Citizen make get requests for property types/entity types/entity sets/schemas
		propertyTypeMetadataLookup();	
	};
	
	private void propertyTypeMetadataLookup() {		
		//You are God
		setIdentity( GOD_UUID );
		PropertyType resultAddressForGod = dms.getPropertyType( ADDRESS );
		PropertyType resultLifeExpectancyForGod = dms.getPropertyType( LIFE_EXPECTANCY );
		System.out.println( resultAddressForGod );
		System.out.println( resultLifeExpectancyForGod );
		
		//You are President
		setIdentity( PRESIDENT_UUID );
		PropertyType resultAddressForPresident = dms.getPropertyType( ADDRESS );
		//President got read access for Address, should have same view as God
		Assert.assertEquals( resultAddressForGod, resultAddressForPresident );
		//President has no read access for Life expectancy
		try{
			PropertyType resultLifeExpectancyForPresident = dms.getPropertyType( LIFE_EXPECTANCY );
			Assert.fail("Unauthorized Exception should have been thrown.");
		} catch ( UnauthorizedException e){
			Assert.assertTrue( e instanceof UnauthorizedException  );
		}

		//You are Citizen
		setIdentity( CITIZEN_UUID );
		//Citizens has no read access
		try{
			PropertyType resultAddressForCitizen = dms.getPropertyType( ADDRESS );
			Assert.fail("Unauthorized Exception should have been thrown.");
		} catch ( UnauthorizedException e){
			Assert.assertTrue( e instanceof UnauthorizedException  );
		}
		try{
			PropertyType resultLifeExpectancyForCitizen = dms.getPropertyType( LIFE_EXPECTANCY );
			Assert.fail("Unauthorized Exception should have been thrown.");
		} catch ( UnauthorizedException e){
			Assert.assertTrue( e instanceof UnauthorizedException  );
		}
	}
	
}
