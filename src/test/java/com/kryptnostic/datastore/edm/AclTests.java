package com.kryptnostic.datastore.edm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.UUIDs.Syncs;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.ODataStorageService;
import com.kryptnostic.datastore.services.GetSchemasRequest.TypeDetails;

public class AclTests extends BootstrapDatastoreWithCassandra {

	protected static final AclEdmManager          aclEdm                 = ds.getContext().getBean( AclEdmManager.class );
	protected static final AclDataService         aclDs                  = ds.getContext().getBean( AclDataService.class );
	protected static final AclODataStorageService aclODsc                = ds.getContext().getBean( AclODataStorageService.class );
	protected static final PermissionService      ps                     = ds.getContext().getBean( PermissionService.class );
	
	protected static final UUID                   GOD_UUID               = UUID.randomUUID();
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
		createData();
		grantRights();
	}
	
	// Look up property types/entity types/entity sets/schemas given acl
	@Test
	public void accessTypes(){
		typeLookup();
		entitiesLookup();
	};

	@Test
	public void modifyTypes(){
		//TODO: Function for updating values in database not implemented
//		modifyValues();
		godGivesRights();
		//President adds property type SPIED_ON, that not even God can access 
		presidentIsWatchingYou();
		//God removes President's permissions after President lets God know about SPIED_ON
		godRemovesRights();
	}
	
	// Delete created property types/entity types/entity sets/schemas - Acl tables should update correspondingly.
	@AfterClass
	public static void resetAcl(){
		//God deletes the nation
	}

	/**
	 * End of JUnit Tests
	 */
	
	private static void createTypes() {
		//God create property types Life expectancy, Address
		PropertyType lifeExpectancy = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( LIFE_EXPECTANCY.getName() )
				.setDatatype( EdmPrimitiveTypeKind.Int32).setMultiplicity( 0 );
		PropertyType address = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( ADDRESS.getName() )
				.setDatatype( EdmPrimitiveTypeKind.String).setMultiplicity( 0 );
		PropertyType position = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( POSITION.getName() )
				.setDatatype( EdmPrimitiveTypeKind.String).setMultiplicity( 0 );
		aclEdm.createPropertyType( GOD_UUID, lifeExpectancy );
		aclEdm.createPropertyType( GOD_UUID, address );
		aclEdm.createPropertyType( GOD_UUID, position );

		//God creates entity type Citizen, creates entity set President, schema
        EntityType citizens = new EntityType().setNamespace( NATION_NAMESPACE ).setName( NATION_CITIZENS.getName() )
                .setKey( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ) ) )
                .setProperties( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        new FullQualifiedName( NATION_NAMESPACE, LIFE_EXPECTANCY.getName() ),
                        new FullQualifiedName( NATION_NAMESPACE, ADDRESS.getName() ),
                        new FullQualifiedName( NATION_NAMESPACE, POSITION.getName() ) ) );
        aclEdm.createEntityType( GOD_UUID, citizens);		
			//*** Do you want to change the createEntitySet function for consistency? i.e. Create an EntitySet object as well.
        EntitySet secretService = new EntitySet().setType( NATION_CITIZENS )
        		.setName( NATION_SECRET_SERVICE )
        		.setTitle( "Every nation would have one");
        aclEdm.createEntitySet( GOD_UUID, secretService );
        	//*** Change createSchema function to remove Acl Id
        aclEdm.createSchema( GOD_UUID, NATION_NAMESPACE, NATION_SCHEMA, ImmutableSet.of( NATION_CITIZENS ) );
	}
	
	private static void createData(){
		int dataLength = 4;
		List<Integer> dataLifeExpectancy = Arrays.asList(80, 42, 63, 76);
		List<String> dataAddress = Arrays.asList(
				"99 Argyle Street Tewksbury, MA 01876", 
				"662 Lake Forest Road Orange Park, FL 32065", 
				"22 Peachtree Road Marysville, OH 43040",
				"7393 South Armstrong Ave. Farmingdale, NY 11735"
				);
		List<String> dataPosition = Arrays.asList("President", "Hacker", "Banker", "Teacher");
		List<String> dataEntitySet = Arrays.asList(null, NATION_SECRET_SERVICE, null, null);
		
		Property employeeId;
		Property lifeExpectancy;
		Property address;
		Property position;
		
		for(int i = 0; i < dataLength; i++){
            employeeId = new Property();
            employeeId.setName( EMPLOYEE_ID );
            employeeId.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ).getFullQualifiedNameAsString() );
            employeeId.setValue( ValueType.PRIMITIVE, UUID.randomUUID() );		
            
            lifeExpectancy = new Property();
            lifeExpectancy.setName( LIFE_EXPECTANCY.getName() );
            lifeExpectancy.setType( LIFE_EXPECTANCY.getFullQualifiedNameAsString() );
            lifeExpectancy.setValue( ValueType.PRIMITIVE, dataLifeExpectancy.get(i) );

            address = new Property();
            address.setName( ADDRESS.getName() );
            address.setType( ADDRESS.getFullQualifiedNameAsString() );
            address.setValue( ValueType.PRIMITIVE, dataAddress.get(i) );

            position = new Property();
            position.setName( POSITION.getName() );
            position.setType( POSITION.getFullQualifiedNameAsString() );
            position.setValue( ValueType.PRIMITIVE, dataPosition.get(i) );

            Entity entity = new Entity();
            entity.setType( NATION_CITIZENS.getFullQualifiedNameAsString() );
            entity.addProperty( employeeId ).addProperty( lifeExpectancy ).addProperty( address ).addProperty( position );
            
            aclODsc.createEntityData( GOD_UUID,
                    Syncs.BASE.getSyncId(),
                    dataEntitySet.get(i), //fix - null should indicate that this is added to default entity set for the type 
                    NATION_CITIZENS,
                    entity );
		}
	}
	
	/**
	 * God grant rights to President and citizens.
	 * God: can do all actions in all types, except the property type SPIED_ON that President will create in later test.
	 * President: 
	 * - Can read, write, modify property types ADDRESS and POSITION, cannot do anything about LIFE_EXPECTANCY
	 * - Can do all actions to entity Type and entity Set
	 * - Can read Schema
	 * Citizen:
	 * - Can read, write ADDRESS, read POSITION, cannot do anything about LIFE_EXPECTANCY
	 * - Can read, write entity Type NATION_CITIZENS, cannot do anything about NATION_SECRET_SERVICE
	 * - Can read Schema
	 */
	private static void grantRights(){
		//Property Types
		ps.addPermission( PRESIDENT_UUID, ImmutableSet.of(ADDRESS, POSITION), ImmutableSet.of("READ", "WRITE", "MODIFY") );
		ps.addPermission( CITIZEN_UUID, ADDRESS, ImmutableSet.of("READ", "WRITE") );
		ps.addPermission( CITIZEN_UUID, POSITION, ImmutableSet.of("READ") );
        //Entity types, entity set   
		ps.addPermission( PRESIDENT_UUID, ImmutableSet.of( NATION_CITIZENS, NATION_SECRET_SERVICE ), ImmutableSet.of("READ", "WRITE", "MODIFY", "GRANT_ACCESS") );
		ps.addPermission( CITIZEN_UUID, NATION_CITIZENS, ImmutableSet.of("READ", "WRITE") );
        //Schema
		ps.addPermission( CITIZEN_UUID, NATION_SCHEMA, ImmutableSet.of("READ") );
	}
	
	// Look up individual types given Acl
	private void typeLookup(){
		//God, President, Citizen make get requests for property types/entity types/entity sets/schemas
		propertyTypeMetadataLookup();
		entityTypeMetadataLookup();
		entitySetMetadataLookup();
		schemaMetadataLookup();		
	};
	
	private void schemaMetadataLookup() {
		EntitySet resultGod = aclEdm.getSchema( GOD_UUID, NATION_SCHEMA.getNamespace(), NATION_SCHEMA.getName(), EnumSet.allOf( TypeDetails.class ) );
		EntitySet resultPresident = aclEdm.getSchema( PRESIDENT_UUID, NATION_SCHEMA.getNamespace(), NATION_SCHEMA.getName(), EnumSet.allOf( TypeDetails.class ) );
		EntitySet resultCitizen = aclEdm.getSchema( CITIZEN_UUID, NATION_SCHEMA.getNamespace(), NATION_SCHEMA.getName(), EnumSet.allOf( TypeDetails.class ) );

		System.out.println( resultGod );
		System.out.println( resultPresident );
		System.out.println( resultCitizen );	
	}

	private void entitySetMetadataLookup() {
		EntitySet resultGod = aclEdm.getPropertyType( GOD_UUID, NATION_CITIZENS, NATION_SECRET_SERVICE );
		EntitySet resultPresident = aclEdm.getPropertyType( PRESIDENT_UUID, NATION_CITIZENS, NATION_SECRET_SERVICE );
		EntitySet resultCitizen = aclEdm.getPropertyType( CITIZEN_UUID, NATION_CITIZENS, NATION_SECRET_SERVICE );

		System.out.println( resultGod );
		System.out.println( resultPresident );
		System.out.println( resultCitizen );	
	}

	private void entityTypeMetadataLookup() {
		//write AssertEquals later
		EntityType resultGod = aclEdm.getEntityType( GOD_UUID, NATION_CITIZENS );
		EntityType resultPresident = aclEdm.getEntityType( PRESIDENT_UUID, NATION_CITIZENS );
		EntityType resultCitizen = aclEdm.getEntityType( CITIZEN_UUID, NATION_CITIZENS );

		System.out.println( resultGod );
		System.out.println( resultPresident );
		System.out.println( resultCitizen );
	}

	private void propertyTypeMetadataLookup() {
		//write AssertEquals later
		PropertyType resultGod = aclEdm.getPropertyType( GOD_UUID, ADDRESS );
		PropertyType resultPresident = aclEdm.getPropertyType( PRESIDENT_UUID, ADDRESS );
		PropertyType resultCitizen = aclEdm.getPropertyType( CITIZEN_UUID, ADDRESS );

		System.out.println( resultGod );
		System.out.println( resultPresident );
		System.out.println( resultCitizen );
		
		//write AssertEquals later
		PropertyType secondResultGod = aclEdm.getPropertyType( GOD_UUID, LIFE_EXPECTANCY );
		PropertyType secondResultPresident = aclEdm.getPropertyType( PRESIDENT_UUID, LIFE_EXPECTANCY );
		PropertyType secondResultCitizen = aclEdm.getPropertyType( CITIZEN_UUID, LIFE_EXPECTANCY );

		System.out.println( secondResultGod );
		System.out.println( secondResultPresident );
		System.out.println( secondResultCitizen );
	}

	// Look up entities given acl
	private void entitiesLookup(){
		//God, President, Citizen make get requests for entities
		Iterable<Multimap<FullQualifiedName, Object>> resultGod = aclDs.getAllEntitiesOfType( GOD_UUID, NATION_CITIZENS);
		Iterable<Multimap<FullQualifiedName, Object>> resultPresident = aclDs.getAllEntitiesOfType( PRESIDENT_UUID, NATION_CITIZENS);
		Iterable<Multimap<FullQualifiedName, Object>> resultCitizen = aclDs.getAllEntitiesOfType( CITIZEN_UUID, NATION_CITIZENS);

		//write AssertEquals later
		for( Multimap<FullQualifiedName, Object> result: resultGod ){
			System.out.println( result );
		}
		for( Multimap<FullQualifiedName, Object> result: resultPresident ){
			System.out.println( result );
		}
		for( Multimap<FullQualifiedName, Object> result: resultCitizen ){
			System.out.println( result );
		}
	}

	private void modifyValues(){
		//These two tests are for testing Acl + modifying values in database
		//TODO: These two tests are empty for now, since there is no function to change values in database as of now.
		changeAddress();
		changePosition();
	}
	
	private void godGivesRights(){
		//God allows Citizen to read LIFE_EXPECTANCY.
		yourFateIsKnown();
		//God allows President to write LIFE_EXPECTANCY.
		//TODO: skipped for now, since modifying values is not implemented in backend yet
//		longLivePresident();
	}
	
	private void changeAddress(){}
	
	private void changePosition(){}

	private void yourFateIsKnown(){
		ps.addPermission( CITIZEN_UUID, LIFE_EXPECTANCY, ImmutableSet.of("READ") );
        
		Iterable<Multimap<FullQualifiedName, Object>> resultPresident = aclDs.getAllEntitiesOfType( PRESIDENT_UUID, NATION_CITIZENS);
		Iterable<Multimap<FullQualifiedName, Object>> resultCitizen = aclDs.getAllEntitiesOfType( CITIZEN_UUID, NATION_CITIZENS);
		
		//write AssertEquals later
		for( Multimap<FullQualifiedName, Object> result: resultPresident ){
			System.out.println( result );
		}
		for( Multimap<FullQualifiedName, Object> result: resultCitizen ){
			System.out.println( result );
		}

	}
	
	private void longLivePresident(){
		ps.addPermission( PRESIDENT_UUID, LIFE_EXPECTANCY, ImmutableSet.of("WRITE") );
	    
		//TODO: President modifies his life expectancy
		
		Iterable<Multimap<FullQualifiedName, Object>> resultPresident = aclDs.getAllEntitiesOfType( PRESIDENT_UUID, NATION_CITIZENS);
		
		//write AssertEquals later
		for( Multimap<FullQualifiedName, Object> result: resultPresident ){
			System.out.println( result );
		}
	}
		
	private void presidentIsWatchingYou(){
		//President adds SPIED_ON property
		PropertyType spiedON = new PropertyType().setNamespace( NATION_NAMESPACE ).setName( SPIED_ON.getName() )
				.setDatatype( EdmPrimitiveTypeKind.Boolean).setMultiplicity( 0 );
		
		aclEdm.createPropertyType( PRESIDENT_UUID, spiedON );
		aclEdm.addPropertyTypesToEntityType(PRESIDENT_UUID, NATION_CITIZENS.getNamespace(), NATION_CITIZENS.getName(), ImmutableSet.of( SPIED_ON ) );
		
		//Make sure that no one can see them except President
		typeLookup();
	}
	
	private void godRemovesRights(){
		//President lets God access property type SPIED_ON
		//TODO: How does backend know it's president setting rights this time?
		ps.addPermission( GOD_UUID, SPIED_ON, ImmutableSet.of("READ") );		

		//Check what everyone can see again.
		typeLookup();
		
		//God removes all of President's rights except reading Schema. In particular, he should not be able to read/write any types.
		//TODO: if one cannot access an entity type, he shouldn't be able to access the entity sets under it either.
		ps.setPermission( PRESIDENT_UUID, ImmutableSet.of(ADDRESS, POSITION, LIFE_EXPECTANCY, SPIED_ON, NATION_CITIZENS), Collections.emptySet() );
		
		//Check what everyone can see again.
		typeLookup();		
	}
	
}
