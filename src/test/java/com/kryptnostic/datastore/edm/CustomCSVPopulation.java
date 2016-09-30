package com.kryptnostic.datastore.edm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.Employee;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.UUIDs.Syncs;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.ODataStorageService;

public class CustomCSVPopulation {
    public static final String               NAMESPACE       = "stressTest";
    protected static final DatastoreServices ds              = new DatastoreServices();
    static EdmManager dms;
    static ODataStorageService odsc;
    
	public static int defaultTypeSize = 0;
	public static List<customPropertyType> defaultTypeList = new ArrayList<customPropertyType>();
	public static List<Integer> multiplicityOfDefaultType = new ArrayList<Integer>();
	
	public static List<customPropertyType> propertyTypesList = new ArrayList<customPropertyType>();
	public static List<String> EntityTypesList = new ArrayList<String>();
	public static List<String> EntitySetsList = new ArrayList<String>();
	public static Map<String, String> EntitySetToType = new HashMap<String, String>();
	
	/**
	 * Custom PropertyType. Will use to generate PropertyType in datastore.
	 * @param name Name of property type to be used in generating datastore PropertyType
	 * @param dataType DataType of property type to be used in generating datastore PropertyType
	 * @param typeInURL Keywords defined in http://www.convertcsv.com/generate-test-data.htm#keywords
	 * @author soarer
	 *
	 */
	private static class customPropertyType{
		private String name;
		private EdmPrimitiveTypeKind dataType;
		private String typeInURL;
		
		public customPropertyType(String name, EdmPrimitiveTypeKind dataType, String typeInURL) {
			this.name = name;
			this.dataType = dataType;
			this.typeInURL = typeInURL;
		}
		
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public EdmPrimitiveTypeKind getDataType() {
			return dataType;
		}
		public void setDataType(EdmPrimitiveTypeKind dataType) {
			this.dataType = dataType;
		}
		public String getTypeInURL() {
			return typeInURL;
		}
		public void setTypeInURL(String typeInURL) {
			this.typeInURL = typeInURL;
		}
	}
	
	public static void LoadDefaultPropertyTypes(){
		defaultTypeList.add( new customPropertyType("age", EdmPrimitiveTypeKind.Int16, "age") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("alpha", EdmPrimitiveTypeKind.String, "alpha") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("birthday", EdmPrimitiveTypeKind.Date, "birthday") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("bool", EdmPrimitiveTypeKind.Boolean, "bool") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("char", EdmPrimitiveTypeKind.String, "char") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("city", EdmPrimitiveTypeKind.String, "city") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("ccnumber", EdmPrimitiveTypeKind.Int64, "ccnumber") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("date", EdmPrimitiveTypeKind.Date, "date") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("digit", EdmPrimitiveTypeKind.Byte, "digit") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("dollar", EdmPrimitiveTypeKind.String, "dollar") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("email", EdmPrimitiveTypeKind.String, "email") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("first", EdmPrimitiveTypeKind.String, "first") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("float", EdmPrimitiveTypeKind.Double, "float") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("gender", EdmPrimitiveTypeKind.String, "gender") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("guid", EdmPrimitiveTypeKind.Guid, "guid") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("integer", EdmPrimitiveTypeKind.Int64, "integer") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("last", EdmPrimitiveTypeKind.String, "last") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("name", EdmPrimitiveTypeKind.String, "name") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("string", EdmPrimitiveTypeKind.String, "string") );
		multiplicityOfDefaultType.add(0);

		defaultTypeList.add( new customPropertyType("street", EdmPrimitiveTypeKind.String, "street") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeList.add( new customPropertyType("state", EdmPrimitiveTypeKind.String, "state") );
		multiplicityOfDefaultType.add(0);
		
		defaultTypeSize = defaultTypeList.size();				
	}
	
	/**
	 * 
	 * @param n Generate n Property Types
	 * Every type would be generated from the default Types
	 * Multiplicities of default Types generated would be recorded, and append to the name of generated property type
 	 * @return
	 */
	public static List<customPropertyType> GeneratePropertyTypes(int n){
		
		Random rand = new Random();
		for (int i = 0; i < n; i++){
			int index = rand.nextInt(defaultTypeSize);
			
			int count = multiplicityOfDefaultType.get( index ) + 1;
			multiplicityOfDefaultType.set( index, count );
			
			String newName = defaultTypeList.get(index).getName() + "-" + count;
			EdmPrimitiveTypeKind dataType = defaultTypeList.get(index).getDataType();
			String typeInURL = defaultTypeList.get(index).getTypeInURL();
			
			propertyTypesList.add( new customPropertyType(newName,dataType,typeInURL) );			
		}
		return propertyTypesList;
	}
	
	public static void GenerateHeader(){
		String JSONvalue = "";
		for(customPropertyType type : propertyTypesList){
			JSONvalue = JSONvalue + type.getTypeInURL() + ",";
		}
		JSONvalue = JSONvalue.substring(0, JSONvalue.length() - 1);
		System.out.println(JSONvalue);
	}
	
	public static void CreatePropertyTypes(){
		for(customPropertyType type : propertyTypesList){
			dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( type.getName() )
					.setDatatype( type.getDataType() ).setMultiplicity( 0 ) );
		}
	}
	
	/**
	 * @param list List of customPropertyType
	 * @param n Create n Entity Types, each with all the existing property types.
	 * @param m Create m Entity Sets for each Entity Type
	 * Default setting:
	 * * Entity Type has 10-character names
	 * * Each Entity Type has 
	 */
	public static void CreateEntityTypes(int n, int m){
		for(int i = 0; i < n; i++){
			//Entity Type of 10-character names
			String entityTypeName = RandomStringUtils.randomAlphabetic(10);
			
	        EntityType entityType = new EntityType().setNamespace( NAMESPACE )
	        		.setName( entityTypeName )
	                .setKey( ImmutableSet.of( new FullQualifiedName( NAMESPACE, "seq" ) ) );
	        //Add property types to entity type
	        
	        Set<FullQualifiedName> setPropertyTypesFQN = new HashSet<FullQualifiedName>();
	        for(customPropertyType propertyType: propertyTypesList){
	        	setPropertyTypesFQN.add( new FullQualifiedName(NAMESPACE, propertyType.getName()) );
	        }
            entityType.setProperties( setPropertyTypesFQN );

	        //Create Entity Type in database
	        dms.createEntityType( entityType );
	        
	        //Update list of custom Entity Types
			EntityTypesList.add( entityTypeName );
			
			//Create entity set
			for(int j = 0; j < m; j++){
				String entitySetName = RandomStringUtils.randomAlphabetic(10);
				//Create entity set
		        dms.createEntitySet( 
		        		new FullQualifiedName( NAMESPACE, entityTypeName),
		                entitySetName,
		                "Random Entity Set " + entitySetName );
		        
		        //Update list of custom Entity Sets
		        EntitySetsList.add( entitySetName );
		        //Update entity set to type map
		        EntitySetToType.put(entitySetName, entityTypeName);
			}
		}
	}
	
	public static void CreateSchema(){
		Set<FullQualifiedName> setOfEntityTypes = new HashSet();
		
		for(String entityTypeName : EntityTypesList){
			setOfEntityTypes.add( new FullQualifiedName(NAMESPACE, entityTypeName) );
		}
		
        dms.createSchema( NAMESPACE,
                "hochung",
                ACLs.EVERYONE_ACL,
                setOfEntityTypes );
	}
	
	public static void WriteCSVtoDB(String location) throws JsonProcessingException, IOException{
		int numOfEntitySets = EntitySetsList.size();
		Random rand = new Random();
		
		CsvMapper mapper = new CsvMapper();
		// important: we need "array wrapping" (see next section) here:
		mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		File csvFile = new File( location ); // or from String, URL etc
		
		MappingIterator<List<Object>> it = mapper.readerFor(new TypeReference<List<Object>>() { }).readValues(csvFile);
		while (it.hasNext()) {
		  Entity entity = new Entity();
		  String entitySetName = EntitySetsList.get( rand.nextInt( numOfEntitySets ) );
		  String entityTypeName = EntitySetToType.get( entitySetName );
		  FullQualifiedName entityTypeFQN = new FullQualifiedName(NAMESPACE, entityTypeName);
		  
          entity.setType( entityTypeFQN.getFullQualifiedNameAsString() );			
		  
          Iterator<Object> rowIterator = it.next().iterator();
		  
		  for(customPropertyType propertyType : propertyTypesList){
              Property property = new Property();
              String propertyName = propertyType.getName();
              
              property.setName( propertyName );
              property.setType( new FullQualifiedName( NAMESPACE, propertyName ).getFullQualifiedNameAsString() );
              property.setValue( ValueType.PRIMITIVE, rowIterator.next() );
              
              entity.addProperty(property);
		  }
		  
          odsc.createEntityData( ACLs.EVERYONE_ACL,
                  Syncs.BASE.getSyncId(),
                  entitySetName,
                  entityTypeFQN,
                  entity );
		}
	}
	public static void main(String args[]) throws JsonProcessingException, IOException{
		//Perhaps drop keyspace to make things cleaner
		//DONE: Load the default property types, from part of the keywords in http://www.convertcsv.com/generate-test-data.htm#keywords
		LoadDefaultPropertyTypes();
		//DONE: Generate a list of custom property types
		GeneratePropertyTypes(10);
		//DONE: Add in a seq column that will be used as partition key
		propertyTypesList.add( new customPropertyType("seq", EdmPrimitiveTypeKind.Int64, "seq") );
		//DONE: Generate value for JSON Header - at this point, go to website http://www.convertcsv.com/generate-test-data.htm to generate csv
		GenerateHeader();
		
		//Input Filename
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter Name of CSV file");
        String nameCSV = scanner.nextLine();
        
		//Start Cassandra and get beans
        ds.sprout("cassandra");
        dms = ds.getContext().getBean( EdmManager.class );
        odsc = ds.getContext().getBean( ODataStorageService.class );
		
        //DONE: Create PropertyType in database
		CreatePropertyTypes();
		//DONE: Create EntityType, and EntitySet for this test in database
		CreateEntityTypes(5, 3);
		//DONE: Create Schema
		CreateSchema();
		
		//Write CSV into database
        //Update path if necessary
		WriteCSVtoDB( "src/test/resources/" + nameCSV + ".csv" );
		
		//End Cassandra
        ds.plowUnder();
        
		//At this point, write shell script to make curl requests to server, and time.
	}
}
