package com.kryptnostic.datastore.edm;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalTime;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmEntityContainer;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.core.edm.EdmEntityContainerImpl;
import org.apache.olingo.commons.core.edm.EdmEntitySetImpl;
import org.apache.olingo.commons.core.edm.EdmEntityTypeImpl;
import org.apache.olingo.commons.core.edm.EdmProviderImpl;
import org.apache.olingo.server.api.ODataApplicationException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.HttpMessageNotWritableException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.dataloom.data.requests.CreateEntityRequest;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.kryptnostic.conductor.rpc.Employee;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.UUIDs.Syncs;
import com.kryptnostic.datastore.converters.IterableCsvHttpMessageConverter;
import com.kryptnostic.datastore.exceptions.BadRequestException;
import com.kryptnostic.datastore.odata.KryptnosticEdmProvider;
import com.kryptnostic.datastore.odata.Transformers;
import com.kryptnostic.datastore.odata.Transformers.EntityTypeTransformer;
import com.kryptnostic.datastore.services.DataService;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.EdmService;
import com.kryptnostic.datastore.services.ODataStorageService;

public class DatastoreTests extends BootstrapDatastoreWithCassandra {

    private static final Multimap<String, Object> m = HashMultimap.create();

    @BeforeClass
    public static void initDatastoreTests() {
        init();
    }

    @Test
    public void testSerialization() throws HttpMessageNotWritableException, IOException {
        IterableCsvHttpMessageConverter converter = new IterableCsvHttpMessageConverter(
                ds.getContext().getBean( EdmService.class ) );
        m.put( new FullQualifiedName( NAMESPACE, SALARY ).getFullQualifiedNameAsString(), 1 );
        m.put( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ).getFullQualifiedNameAsString(), UUID.randomUUID() );
        m.put( new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ).getFullQualifiedNameAsString(), "Master Chief" );
        converter.write( ImmutableList.of( m ), null, null, new HttpOutputMessage() {

            @Override
            public HttpHeaders getHeaders() {
                return new HttpHeaders();
            }

            @Override
            public OutputStream getBody() throws IOException {
                return System.out;
            }
        } );
    }

    @Test
    public void testCreateEntityType() {
        ODataStorageService esc = ds.getContext().getBean( ODataStorageService.class );
        Property empId = new Property();
        Property empName = new Property();
        Property empTitle = new Property();
        Property empSalary = new Property();
        empId.setName( EMPLOYEE_ID );
        empId.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ).getFullQualifiedNameAsString() );
        empId.setValue( ValueType.PRIMITIVE, UUID.randomUUID() );

        empName.setName( EMPLOYEE_NAME );
        empName.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ).getFullQualifiedNameAsString() );
        empName.setValue( ValueType.PRIMITIVE, "Kung Fury" );

        empTitle.setName( EMPLOYEE_TITLE );
        empTitle.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ).getFullQualifiedNameAsString() );
        empTitle.setValue( ValueType.PRIMITIVE, "Kung Fu Master" );

        empSalary.setName( SALARY );
        empSalary.setType( new FullQualifiedName( NAMESPACE, SALARY ).getFullQualifiedNameAsString() );
        empSalary.setValue( ValueType.PRIMITIVE, Long.MAX_VALUE );

        Entity e = new Entity();
        e.setType( ENTITY_TYPE.getFullQualifiedNameAsString() );
        e.addProperty( empId ).addProperty( empName ).addProperty( empTitle ).addProperty( empSalary );
        esc.createEntityData( ACLs.EVERYONE_ACL,
                Syncs.BASE.getSyncId(),
                ENTITY_SET_NAME,
                ENTITY_TYPE,
                e );

        // esc.readEntityData( edmEntitySet, keyParams );
    }

    // @Test
    public void polulateEmployeeCsv() throws IOException {
        ODataStorageService esc = ds.getContext().getBean( ODataStorageService.class );
        Property employeeId;
        Property employeeName;
        Property employeeTitle;
        Property employeeDept;
        Property employeeSalary;

        try ( FileReader fr = new FileReader( "src/test/resources/employees.csv" );
                BufferedReader br = new BufferedReader( fr ) ) {

            String line;
            while ( ( line = br.readLine() ) != null ) {
                Employee employee = Employee.EmployeeCsvReader.getEmployee( line );
                System.out.println( employee.toString() );

                employeeId = new Property();
                employeeId.setName( EMPLOYEE_ID );
                employeeId.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ).getFullQualifiedNameAsString() );
                employeeId.setValue( ValueType.PRIMITIVE, UUID.randomUUID() );

                employeeName = new Property();
                employeeName.setName( EMPLOYEE_NAME );
                employeeName
                        .setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ).getFullQualifiedNameAsString() );
                employeeName.setValue( ValueType.PRIMITIVE, employee.getName() );

                employeeTitle = new Property();
                employeeTitle.setName( EMPLOYEE_TITLE );
                employeeTitle
                        .setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ).getFullQualifiedNameAsString() );
                employeeTitle.setValue( ValueType.PRIMITIVE, employee.getTitle() );

                employeeDept = new Property();
                employeeDept.setName( EMPLOYEE_DEPT );
                employeeDept
                        .setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ).getFullQualifiedNameAsString() );
                employeeDept.setValue( ValueType.PRIMITIVE, employee.getDept() );

                employeeSalary = new Property();
                employeeSalary.setName( SALARY );
                employeeSalary.setType( new FullQualifiedName( NAMESPACE, SALARY ).getFullQualifiedNameAsString() );
                employeeSalary.setValue( ValueType.PRIMITIVE, (long) employee.getSalary() );

                Entity entity = new Entity();
                entity.setType( ENTITY_TYPE.getFullQualifiedNameAsString() );
                entity.addProperty( employeeId )
                        .addProperty( employeeName )
                        .addProperty( employeeTitle )
                        .addProperty( employeeDept )
                        .addProperty( employeeSalary );

                esc.createEntityData( ACLs.EVERYONE_ACL,
                        Syncs.BASE.getSyncId(),
                        ENTITY_SET_NAME,
                        ENTITY_TYPE,
                        entity );

                // Created by Ho Chung for testing different entity types
                // add entityType "employeeMars"
                esc.createEntityData( ACLs.EVERYONE_ACL,
                        Syncs.BASE.getSyncId(),
                        ENTITY_SET_NAME,
                        ENTITY_TYPE_MARS,
                        entity );
                // add entityType "employeeSaturn"
                esc.createEntityData( ACLs.EVERYONE_ACL,
                        Syncs.BASE.getSyncId(),
                        ENTITY_SET_NAME,
                        ENTITY_TYPE_SATURN,
                        entity );

            }
        }
    }

    // @Test
    public void testRead() {
        Set<FullQualifiedName> properties = ImmutableSet.of(
                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ),
                new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ),
                new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ),
                new FullQualifiedName( NAMESPACE, SALARY ) );

        ODataStorageService esc = ds.getContext().getBean( ODataStorageService.class );
        EdmManager dms = ds.getContext().getBean( EdmManager.class );
        KryptnosticEdmProvider provider = new KryptnosticEdmProvider( dms );
        Edm edm = new EdmProviderImpl( provider );

        CsdlEntityContainerInfo info = new CsdlEntityContainerInfo().setContainerName( ENTITY_TYPE );
        EdmEntityContainer edmEntityContainer = new EdmEntityContainerImpl( edm, provider, info );

        CsdlEntityType csdlEntityType = new EntityTypeTransformer( dms ).transform(
                new EntityType()
                        .setName( ENTITY_TYPE.getName() )
                        .setNamespace( ENTITY_TYPE.getNamespace() )
                        .setProperties( properties )
                        .setKey( ImmutableSet.of() ) );
        EdmEntityType edmEntityType = new EdmEntityTypeImpl( edm, ENTITY_TYPE, csdlEntityType );

        CsdlEntitySet csdlEntitySet = Transformers.transform(
                new EntitySet().setName( ENTITY_SET_NAME ).setTitle( ENTITY_SET_NAME ).setType( ENTITY_TYPE ) );
        EdmEntitySet edmEntitySet = new EdmEntitySetImpl( edm, edmEntityContainer, csdlEntitySet );

        try {
            EntityCollection ec = esc.readEntitySetData( edmEntitySet );
            ec.forEach( currEntity -> System.out.println( currEntity ) );
        } catch ( ODataApplicationException e ) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAddPropertyTypeToEntityType() {
        // Desired result: Properties EMPLOYEE_COUNTRY, EMPLOYEE_WEIGHT are added to ENTITY_TYPE (Employees)
        final String EMPLOYEE_COUNTRY = "employee_country";
        final String EMPLOYEE_WEIGHT = "employee_weight";

        EdmManager dms = ds.getContext().getBean( EdmManager.class );

        try {
            dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( EMPLOYEE_COUNTRY )
                    .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 ) );
        } catch ( IllegalArgumentException e ) {
            // Only acceptable exception is property type already exists
            Assert.assertEquals( PROPERTY_TYPE_EXISTS_MSG, e.getMessage() );
        }
        ;

        try {
            dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( EMPLOYEE_WEIGHT )
                    .setDatatype( EdmPrimitiveTypeKind.Int32 ).setMultiplicity( 0 ) );
        } catch ( IllegalArgumentException e ) {
            // Only acceptable exception is property type already exists
            Assert.assertEquals( PROPERTY_TYPE_EXISTS_MSG, e.getMessage() );
        }

        Set<FullQualifiedName> properties = new HashSet<>();
        properties.add( new FullQualifiedName( NAMESPACE, EMPLOYEE_COUNTRY ) );
        properties.add( new FullQualifiedName( NAMESPACE, EMPLOYEE_WEIGHT ) );

        dms.addPropertyTypesToEntityType( ENTITY_TYPE.getNamespace(), ENTITY_TYPE.getName(), properties );
    }

    @Test
    public void testAddExistingPropertyTypeToEntityType() {
        // Action: Property EMPLOYEE_ID is added to ENTITY_TYPE (Employees)
        // Desired result: Since property is already part of ENTITY_TYPE, nothing should happen
        EdmManager dms = ds.getContext().getBean( EdmManager.class );

        Set<FullQualifiedName> properties = new HashSet<>();
        properties.add( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ) );

        dms.addPropertyTypesToEntityType( ENTITY_TYPE.getNamespace(), ENTITY_TYPE.getName(), properties );
    }

    @Test(
        expected = IllegalArgumentException.class )
    public void testAddPhantomPropertyTypeToEntityType() {
        // Action: Add Property EMPLOYEE_HEIGHT to ENTITY_TYPE (Employees)
        // Desired result: Since property does not exist, Bad Request Exception should be thrown
        final String EMPLOYEE_HEIGHT = "employee-height";

        EdmManager dms = ds.getContext().getBean( EdmManager.class );

        Set<FullQualifiedName> properties = new HashSet<>();
        properties.add( new FullQualifiedName( NAMESPACE, EMPLOYEE_HEIGHT ) );

        dms.addPropertyTypesToEntityType( ENTITY_TYPE.getNamespace(), ENTITY_TYPE.getName(), properties );
    }

    @Test
    public void testAddPropertyToSchema() {
        final String EMPLOYEE_TOENAIL_LENGTH = "employee-toenail-length";
        final String EMPLOYEE_FINGERNAIL_LENGTH = "employee-fingernail-length";

        EdmManager dms = ds.getContext().getBean( EdmManager.class );
        try {
            dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( EMPLOYEE_TOENAIL_LENGTH )
                    .setDatatype( EdmPrimitiveTypeKind.Int32 ).setMultiplicity( 0 ) );
        } catch ( IllegalArgumentException e ) {
            // Only acceptable exception is property type already exists
            Assert.assertEquals( PROPERTY_TYPE_EXISTS_MSG, e.getMessage() );
        }

        try {
            dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( EMPLOYEE_FINGERNAIL_LENGTH )
                    .setDatatype( EdmPrimitiveTypeKind.Int32 ).setMultiplicity( 0 ) );
        } catch ( IllegalArgumentException e ) {
            // Only acceptable exception is property type already exists
            Assert.assertEquals( PROPERTY_TYPE_EXISTS_MSG, e.getMessage() );
        }
        // Add new property to Schema
        Set<FullQualifiedName> newProperties = new HashSet<>();
        newProperties.add( new FullQualifiedName( NAMESPACE, EMPLOYEE_TOENAIL_LENGTH ) );
        newProperties.add( new FullQualifiedName( NAMESPACE, EMPLOYEE_FINGERNAIL_LENGTH ) );
        dms.addPropertyTypesToSchema( NAMESPACE, SCHEMA_NAME, newProperties );

        // Add existing property to Schema
        dms.addPropertyTypesToSchema( NAMESPACE,
                SCHEMA_NAME,
                ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ) ) );

        // Add non-existing property to Schema
        Throwable caught = null;
        try {
            dms.addPropertyTypesToSchema( NAMESPACE,
                    SCHEMA_NAME,
                    ImmutableSet.of( new FullQualifiedName( NAMESPACE, "employee-facebook-url" ) ) );
        } catch ( Throwable t ) {
            caught = t;
        }
        assertNotNull( caught );
        assertSame( IllegalArgumentException.class, caught.getClass() );
    }

    @Test
    public void removePropertyTypes() {
        // Action: Add Property EMPLOYEE_HAIRLENGTH to ENTITY_TYPE (Employees), and EMPLOYEE_EYEBROW_LENGTH to Schema,
        // then remove them
        // Desired result: Schemas and Entity_Types tables should look the same as before, without any trace of
        // EMPLOYEE_HAIRLENGTH and EMPLOYEE_EYEBROW_LENGTH
        // Property_Types and lookup table should be updated.
        final String EMPLOYEE_HAIR_LENGTH = "employee_hair_length";
        final String EMPLOYEE_EYEBROW_LENGTH = "employee_eyebrow_length";

        EdmManager dms = ds.getContext().getBean( EdmManager.class );
        try {
            dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( EMPLOYEE_HAIR_LENGTH )
                    .setDatatype( EdmPrimitiveTypeKind.Int32 ).setMultiplicity( 0 ) );
        } catch ( IllegalArgumentException e ) {
            // Only acceptable exception is property type already exists
            Assert.assertEquals( PROPERTY_TYPE_EXISTS_MSG, e.getMessage() );
        }

        try {
            dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( EMPLOYEE_EYEBROW_LENGTH )
                    .setDatatype( EdmPrimitiveTypeKind.Int32 ).setMultiplicity( 0 ) );
        } catch ( IllegalArgumentException e ) {
            // Only acceptable exception is property type already exists
            Assert.assertEquals( PROPERTY_TYPE_EXISTS_MSG, e.getMessage() );
        }
        dms.addPropertyTypesToEntityType( ENTITY_TYPE.getNamespace(),
                ENTITY_TYPE.getName(),
                ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_HAIR_LENGTH ) ) );
        dms.addPropertyTypesToSchema( NAMESPACE,
                SCHEMA_NAME,
                ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_EYEBROW_LENGTH ) ) );

        dms.removePropertyTypesFromEntityType( ENTITY_TYPE.getNamespace(),
                ENTITY_TYPE.getName(),
                ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_HAIR_LENGTH ) ) );
        dms.removePropertyTypesFromSchema( NAMESPACE,
                SCHEMA_NAME,
                ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_EYEBROW_LENGTH ) ) );
    }

    // @Test
    public void populateEmployeeCsvViaDataService() throws IOException {
        // Populate data of entity type EMPLOYEE, via DataService
        DataService dataService = ds.getContext().getBean( DataService.class );

        Random rand = new Random();

        FullQualifiedName EMPLOYEE_ID_FQN = new FullQualifiedName( NAMESPACE, EMPLOYEE_ID );
        FullQualifiedName EMPLOYEE_NAME_FQN = new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME );
        FullQualifiedName EMPLOYEE_TITLE_FQN = new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE );
        FullQualifiedName EMPLOYEE_DEPT_FQN = new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT );
        FullQualifiedName EMPLOYEE_SALARY_FQN = new FullQualifiedName( NAMESPACE, SALARY );
        Set<FullQualifiedName> authorizedProperties = ImmutableSet.of(
                EMPLOYEE_ID_FQN,
                EMPLOYEE_NAME_FQN,
                EMPLOYEE_TITLE_FQN,
                EMPLOYEE_DEPT_FQN,
                EMPLOYEE_SALARY_FQN );

        Set<SetMultimap<FullQualifiedName, Object>> entities = new HashSet<>();

        try ( FileReader fr = new FileReader( "src/test/resources/employees.csv" );
                BufferedReader br = new BufferedReader( fr ) ) {

            String line;
            int count = 0;
            int paging_constant = 100;
            while ( ( line = br.readLine() ) != null ) {
                Employee employee = Employee.EmployeeCsvReader.getEmployee( line );
                System.out.println( employee.toString() );

                SetMultimap<FullQualifiedName, Object> entity = HashMultimap.create();

                entity.put( EMPLOYEE_ID_FQN, UUID.randomUUID() );
                entity.put( EMPLOYEE_NAME_FQN, employee.getName() );
                entity.put( EMPLOYEE_TITLE_FQN, employee.getTitle() );
                entity.put( EMPLOYEE_DEPT_FQN, employee.getDept() );
                entity.put( EMPLOYEE_SALARY_FQN, employee.getSalary() );

                if ( count++ < paging_constant ) {
                    entities.add( entity );
                } else {
                    CreateEntityRequest createEntityRequest = new CreateEntityRequest(
                            Optional.of( "Employees_set_" + rand.nextInt( 10 ) ),
                            ENTITY_TYPE,
                            entities,
                            Optional.absent(),
                            Optional.absent() );

                    dataService.createEntityData( createEntityRequest, authorizedProperties );

                    entities = new HashSet<>();
                    count = 0;
                }
            }
        }
    }

    @Test
    public void testWriteDifferentDataTypes() throws IOException {
        DataService dataService = ds.getContext().getBean( DataService.class );

        FullQualifiedName entityTypeFqn = new FullQualifiedName(
                NAMESPACE,
                "datawritetest_" + RandomStringUtils.randomAlphanumeric( 10 ).toLowerCase() );
        // Testing all the property types supported by CassandraEdmMapping::getCassandraType
        Map<FullQualifiedName, EdmPrimitiveTypeKind> propertyTypeMap = createEntityTypeAndPropertyTypes( entityTypeFqn,
                ImmutableSet.of(
                        EdmPrimitiveTypeKind.Binary,
                        EdmPrimitiveTypeKind.Boolean,
                        EdmPrimitiveTypeKind.Byte,
                        EdmPrimitiveTypeKind.Date,
                        EdmPrimitiveTypeKind.DateTimeOffset,
                        EdmPrimitiveTypeKind.Decimal,
                        EdmPrimitiveTypeKind.Double,
                        EdmPrimitiveTypeKind.Duration,
                        EdmPrimitiveTypeKind.Guid,
                        EdmPrimitiveTypeKind.Int16,
                        EdmPrimitiveTypeKind.Int32,
                        EdmPrimitiveTypeKind.Int64,
                        EdmPrimitiveTypeKind.String,
                        EdmPrimitiveTypeKind.SByte,
                        EdmPrimitiveTypeKind.Single,
                        EdmPrimitiveTypeKind.TimeOfDay ) );

        writeRandomData( dataService, entityTypeFqn, propertyTypeMap, 10 );
    }

    private Map<FullQualifiedName, EdmPrimitiveTypeKind> createEntityTypeAndPropertyTypes(
            FullQualifiedName entityTypeFqn,
            Set<EdmPrimitiveTypeKind> edmTypes ) {
        EdmManager dms = ds.getContext().getBean( EdmManager.class );

        String prefix = "property_";
        String propertyName;
        Map<FullQualifiedName, EdmPrimitiveTypeKind> propertyTypeMap = new HashMap<>();

        for ( EdmPrimitiveTypeKind edmType : edmTypes ) {
            propertyName = prefix + edmType.toString().toLowerCase() + "_"
                    + RandomStringUtils.randomAlphanumeric( 10 ).toLowerCase();
            dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( propertyName )
                    .setDatatype( edmType ).setMultiplicity( 0 ) );
            propertyTypeMap.put( new FullQualifiedName( NAMESPACE, propertyName ), edmType );
        }

        EntityType entityType = new EntityType().setNamespace( entityTypeFqn.getNamespace() )
                .setName( entityTypeFqn.getName() )
                .setKey( ImmutableSet.of( propertyTypeMap.keySet().iterator().next() ) )
                .setProperties( propertyTypeMap.keySet() );
        dms.createEntityType( principal, entityType );

        return propertyTypeMap;
    }

    private void writeRandomData(
            DataService dataService,
            FullQualifiedName entityTypeFqn,
            Map<FullQualifiedName, EdmPrimitiveTypeKind> propertyTypeMap,
            int n ) {
        Random rand = new Random();
        Set<SetMultimap<FullQualifiedName, Object>> entities = new HashSet<>();

        for ( int i = 0; i < n; i++ ) {
            SetMultimap<FullQualifiedName, Object> entity = HashMultimap.create();
            for ( Map.Entry<FullQualifiedName, EdmPrimitiveTypeKind> entry : propertyTypeMap.entrySet() ) {
                entity.put( entry.getKey(), generateValueOfType( rand, entry.getValue() ) );
            }
            entities.add( entity );
        }

        CreateEntityRequest createEntityRequest = new CreateEntityRequest(
                Optional.absent(),
                entityTypeFqn,
                entities,
                Optional.absent(),
                Optional.absent() );

        dataService.createEntityData( createEntityRequest, propertyTypeMap.keySet() );

    }

    private Object generateValueOfType( Random rand, EdmPrimitiveTypeKind edmType ) {
        // Following
        // http://docs.oasis-open.org/odata/odata/v4.0/errata03/os/complete/part3-csdl/odata-v4.0-errata03-os-part3-csdl-complete.html#_Toc453752517
        Object value;
        switch ( edmType ) {
            case Binary:
                value = RandomStringUtils.randomAlphanumeric( 10 );
                break;
            case Boolean:
                value = rand.nextBoolean();
                break;
            case Byte:
                // This will require special processing :-/
                value = rand.nextInt( 8 );
                break;
            case Date:
                value = ( 1000 + rand.nextInt( 2000 ) ) + "-" + ( 1 + rand.nextInt( 12 ) ) + "-"
                        + ( 1 + rand.nextInt( 28 ) );
                break;
            case DateTimeOffset:
                value = Instant.now().toString();
                break;
            case Decimal:
                value = new BigDecimal( Math.random() );
                break;
            case Double:
                value = rand.nextDouble();
                break;
            case Duration:
                value = rand.nextInt() + "d" + rand.nextInt( 24 ) + "h" + rand.nextInt( 60 ) + "m" + rand.nextInt( 60 )
                        + "m";
                break;
            case Guid:
                value = UUID.randomUUID();
                break;
            case Int16:
                value = (short) rand.nextInt( Short.MAX_VALUE + 1 );
                break;
            case Int32:
                value = rand.nextInt();
                break;
            case Int64:
                value = rand.nextLong();
                break;
            case String:
                value = RandomStringUtils.randomAlphanumeric( 10 );
                break;
            case SByte:
                value = rand.nextInt( 256 ) - 128;
                break;
            case Single:
                value = rand.nextFloat();
                break;
            case TimeOfDay:
                value = rand.nextInt( 24 ) + ":" + rand.nextInt( 60 ) + ":" + rand.nextInt( 60 ) + "."
                        + rand.nextInt( 1000 );
                break;
            default:
                value = RandomStringUtils.randomAlphanumeric( 10 );
                break;
        }

        // Pretend that Jackson parsed this from user input
        ObjectMapper mapper = new ObjectMapper();
        String serialized;
        Object deserialized = null;
        try {
            serialized = mapper.writeValueAsString( value );
            // Pretend we are doing raw data binding
            deserialized = mapper.readValue( serialized, Object.class );
        } catch ( Exception e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return deserialized;
    }
}
