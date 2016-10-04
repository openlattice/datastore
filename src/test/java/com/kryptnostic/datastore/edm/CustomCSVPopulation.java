package com.kryptnostic.datastore.edm;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.file.Files;
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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.UUIDs.Syncs;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.services.DataService;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.ODataStorageService;

public class CustomCSVPopulation {
	public static int numPropertyTypes;
	public static int numEntityTypes;
	public static int numEntitySets;
	public static int numRows;
	
	public static String individualResultLoc = "src/test/resources/allResult.txt";
	public static String averageResultLoc = "src/test/resources/averageResult.txt";
	
    public static final String               NAMESPACE       = "stressTest";
    protected static final DatastoreServices ds              = new DatastoreServices();
    static EdmManager dms;
    static ODataStorageService odsc;
    static DataService dataService;
    
	public static int defaultTypeSize = 0;
	public static List<customPropertyType> defaultTypeList = new ArrayList<customPropertyType>();
	
	public static List<customPropertyType> propertyTypesList = new ArrayList<customPropertyType>();
	public static CsvSchema csvSchema;
	public static List<String> EntityTypesList = new ArrayList<String>();
	public static List<String> EntitySetsList = new ArrayList<String>();
	public static Map<String, String> EntitySetToType = new HashMap<String, String>();	
	
	//Random
	public static Map<String, Supplier> RandomGenerator = new HashMap<String, Supplier>();
	
	//Partition Key Count
	public static int partitionKey = 0;
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
		private String keyword;
		private Callable randomGenCallable;
		private String javaTypeName;
		
		public customPropertyType(String name, EdmPrimitiveTypeKind dataType, String keyword, Callable randomGenCallable, String javaTypeName) {
			this.name = name;
			this.dataType = dataType;
			this.keyword = keyword;
			this.randomGenCallable = randomGenCallable;
			this.javaTypeName = javaTypeName;
		}
		
		public String getJavaTypeName() {
			return javaTypeName;
		}

		public String getKeyword() {
			return keyword;
		}

		public String getName() {
			return name;
		}

		public EdmPrimitiveTypeKind getDataType() {
			return dataType;
		}
		
		public Callable getRandomGenCallable(){
			return randomGenCallable;
		}
		public Object getRandom() throws Exception{
			return randomGenCallable.call();
		}
	}
	
	public static void LoadDefaultPropertyTypes(){
		defaultTypeList.add( new customPropertyType("age", EdmPrimitiveTypeKind.Int32, "age", () -> (new Random()).nextInt(120), "Integer") );
		
		defaultTypeList.add( new customPropertyType("alpha", EdmPrimitiveTypeKind.String, "alpha", () -> RandomStringUtils.randomAlphabetic(8), "String" ) );
/**			
		defaultTypeList.add( new customPropertyType("bool", EdmPrimitiveTypeKind.Boolean, "bool", () -> (new Random()).nextBoolean(), "Boolean" ) );
		multiplicityOfDefaultType.add(0);
*/	
		defaultTypeList.add( new customPropertyType("char", EdmPrimitiveTypeKind.String, "char", () -> RandomStringUtils.randomAlphabetic(1), "String" ) );
/**		
		defaultTypeList.add( new customPropertyType("digit", EdmPrimitiveTypeKind.Int32, "digit", () -> (new Random()).nextInt(9), "Integer" ) );
		multiplicityOfDefaultType.add(0);
*/	
		defaultTypeList.add( new customPropertyType("float", EdmPrimitiveTypeKind.Double, "float", () -> (new Random()).nextFloat(), "Double" ) );
		
		defaultTypeList.add( new customPropertyType("guid", EdmPrimitiveTypeKind.Guid, "guid", () -> UUID.randomUUID(), "UUID" ) );
		
		defaultTypeList.add( new customPropertyType("integer", EdmPrimitiveTypeKind.Int32, "integer", () -> (new Random()).nextInt(123456), "Integer" ) );
		
		defaultTypeList.add( new customPropertyType("string", EdmPrimitiveTypeKind.String, "string", () -> RandomStringUtils.randomAscii(10), "String" ) );
				
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
		numPropertyTypes = n;
		
		Random rand = new Random();
		for (int i = 0; i < n; i++){
			int index = rand.nextInt(defaultTypeSize);
			
			customPropertyType propertyType = defaultTypeList.get(index);
			String newName = propertyType.getName() + "-" + rand.nextLong();
			EdmPrimitiveTypeKind dataType = propertyType.getDataType();
			String keyword = propertyType.getKeyword();
			Callable randomGenCallable = propertyType.getRandomGenCallable();
			String javaTypeName = propertyType.getJavaTypeName();
			
			propertyTypesList.add( new customPropertyType(newName,dataType,keyword,randomGenCallable,javaTypeName) );			
		}
		return propertyTypesList;
	}
	
	public static void GenerateCSV(int n, String location) throws Exception{
		numRows = n;
		//Build CSV Schema
		CsvSchema.Builder schemaBuilder = CsvSchema.builder();
		for (customPropertyType type: propertyTypesList){
			schemaBuilder.addColumn(type.getName());
		}
		csvSchema = schemaBuilder.build();
		
		//Write to CSV
        CsvMapper mapper = new CsvMapper();
        ObjectWriter myObjectWriter = mapper.writer(csvSchema);
        
        File tempFile = new File(location);
        if( tempFile.exists() ){
        	tempFile.delete();
        }
        tempFile.createNewFile();
        
        FileOutputStream tempFileOutputStream = new FileOutputStream(tempFile);
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(tempFileOutputStream, 1024);
        OutputStreamWriter writerOutputStream = new OutputStreamWriter(bufferedOutputStream, "UTF-8");
        
        List<List<Object>> values = new ArrayList<List<Object>>();
        for(int i = 0; i < numRows; i++){
        	List<Object> rowValues = new ArrayList<Object>();
        	for( customPropertyType type : propertyTypesList){
        		rowValues.add( type.getRandom() );
        	}
        	values.add(rowValues);
        }
        myObjectWriter.writeValue(writerOutputStream, values);
		System.out.println("CSV generated \n");
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
		numEntityTypes = n;
		numEntitySets = m;
		for(int i = 0; i < numEntityTypes; i++){
			//Entity Type of 10-character names
			String entityTypeName = RandomStringUtils.randomAlphabetic(10);
			
	        EntityType entityType = new EntityType().setNamespace( NAMESPACE )
	        		.setName( entityTypeName )
	                .setKey( ImmutableSet.of( new FullQualifiedName( NAMESPACE, "key" ) ) );
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
			for(int j = 0; j < numEntitySets; j++){
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
	
	private static Object TypeConversion(String str, String type){
		//Convert string to the corresponding type, guaranteed that the string can be converted to that cype.
		if(type.equals("Integer")){
			return Integer.parseInt(str);
		}else if(type.equals("Boolean")){
			return Boolean.parseBoolean(str);			
		}else if(type.equals("Byte")){
			return Byte.parseByte(str);
		}else if(type.equals("Double")){
			return Double.parseDouble(str);
		}else if(type.equals("UUID")){
			return UUID.fromString(str);
		}else if(type.equals("Long")){
			return Long.parseLong(str);
		}else{
			return str;
		}		
	}
	
	public static void WriteCSVtoDB(String location) throws JsonProcessingException, IOException{
		int numOfEntitySets = EntitySetsList.size();
		Random rand = new Random();
		
		CsvMapper mapper = new CsvMapper();
		// important: we need "array wrapping" (see next section) here:
		mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		File csvFile = new File( location ); // or from String, URL etc
		
		MappingIterator<Map<String,String>> it = mapper.readerFor(new TypeReference<Map<String,String>>() { }).with(csvSchema).readValues(csvFile);
		while (it.hasNext()) {
		  Entity entity = new Entity();
		  String entitySetName = EntitySetsList.get( rand.nextInt( numOfEntitySets ) );
		  String entityTypeName = EntitySetToType.get( entitySetName );
		  FullQualifiedName entityTypeFQN = new FullQualifiedName(NAMESPACE, entityTypeName);
		  
          entity.setType( entityTypeFQN.getFullQualifiedNameAsString() );
          Map<String, String> map = it.next();
          
		  for(customPropertyType propertyType : propertyTypesList){
              Property property = new Property();
              String propertyName = propertyType.getName();
              
              property.setName( propertyName );
              property.setType( new FullQualifiedName( NAMESPACE, propertyName ).getFullQualifiedNameAsString() );
              property.setValue( ValueType.PRIMITIVE, TypeConversion(map.get(propertyName), propertyType.getJavaTypeName() ));
              
              entity.addProperty(property);
		  } 
          odsc.createEntityData( ACLs.EVERYONE_ACL,
                  Syncs.BASE.getSyncId(),
                  entitySetName,
                  entityTypeFQN,
                  entity );
		}
	}
	/**
	 * Benchmarking getAllEntitiesOfType
	 * @param numTest
	 * @throws IOException 
	 */
	public static void TimeGetAllEntitiesOfType(int numTest) throws IOException{
		//Initialize file writers
		File fileAll = new File(individualResultLoc);
		FileWriter fwAll = new FileWriter(fileAll.getAbsoluteFile());
		BufferedWriter bwAll = new BufferedWriter(fwAll);
		
		File fileAverage = new File(averageResultLoc);
		FileWriter fwAverage = new FileWriter(fileAverage.getAbsoluteFile());
		BufferedWriter bwAverage = new BufferedWriter(fwAverage);
		
		bwAll.write("========================================================== \n");
		bwAll.write("Testing: getAllEntitiesOfType \n");
		bwAll.write("Number of Columns: " + numPropertyTypes + " \n");
		bwAll.write("Number of Rows: " + numRows + " \n");
		bwAll.write("Number of Entity Types: " + numEntityTypes + " \n");
		bwAll.write("Number of Entity Sets: " + numEntitySets + " \n");
		bwAll.write("========================================================== \n");
		bwAll.write("Test #, Time elapsed (ms) \n");
		
		bwAverage.write("========================================================== \n");
		bwAverage.write("Testing: getAllEntitiesOfType \n");
		bwAverage.write("Number of Columns: " + numPropertyTypes + " \n");
		bwAverage.write("Number of Rows: " + numRows + " \n");
		bwAverage.write("Number of Entity Types: " + numEntityTypes + " \n");
		bwAverage.write("Number of Entity Sets: " + numEntitySets + " \n");
		bwAverage.write("========================================================== \n");

		//Actual testing
		float totalTime = 0;
		
		for (int i = 0; i < numTest; i++){			
			//Decide which EntityType to look up
			String entityTypeName = EntityTypesList.get( (new Random()).nextInt(EntityTypesList.size()) );
			//Make request
			Stopwatch stopwatch = Stopwatch.createStarted();
			Iterable<Multimap<FullQualifiedName,Object>> result = dataService.readAllEntitiesOfType( new FullQualifiedName( NAMESPACE, entityTypeName ) );
			//print result
			stopwatch.stop();
			
			totalTime += stopwatch.elapsed(TimeUnit.MILLISECONDS);
			
			bwAll.write(i + "," + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " \n");
		}
		
		bwAverage.write("Number of tests: " + numTest + " \n");
		bwAverage.write("Average Time (ms):" + totalTime/numTest + " \n");
		
		bwAll.close();
		bwAverage.close();
	}
	@Test
	public static void main(String args[]) throws Exception{
		//Perhaps drop keyspace to make things cleaner
		LoadDefaultPropertyTypes();
		GeneratePropertyTypes(20);
		//Add in a key column that will be used as partition key
		propertyTypesList.add( new customPropertyType("key", EdmPrimitiveTypeKind.Int32, "key", () -> ++partitionKey, "Integer" ) );

		try {
			GenerateCSV(20000, "src/test/resources/stressTest.csv");
		} catch (Exception e) {
			e.printStackTrace();
		}		

		ds.sprout("cassandra");
        dms = ds.getContext().getBean( EdmManager.class );
        odsc = ds.getContext().getBean( ODataStorageService.class );
        dataService = ds.getContext().getBean( DataService.class );
		
        //Create PropertyType, Entity Types, Entity Sets in database
		CreatePropertyTypes();
		//Create 3 EntityTypes, each with 2 EntitySets for this test in database
		CreateEntityTypes(3, 2);
		CreateSchema();
		
		WriteCSVtoDB( "src/test/resources/stressTest.csv" );
		
		System.out.println("TEST STARTS");
		//Time getAllEntitiesOfType 10 times
		TimeGetAllEntitiesOfType(10);
		//Go to src/test/resources/{allResult.text, averageResult.txt} for test results.
		
        ds.plowUnder();        
        System.out.println("TEST DONE");
        
	}
}
