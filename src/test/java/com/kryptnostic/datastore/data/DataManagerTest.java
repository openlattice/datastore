package com.kryptnostic.datastore.data;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.kryptnostic.conductor.rpc.Employee;
import com.kryptnostic.datastore.cassandra.CassandraPropertyReader;
import com.kryptnostic.datastore.services.CassandraDataManager;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.pods.CassandraPod;

public class DataManagerTest {
    // WARNING: TestPod creates the CassandraDataManager bean, whose keyspace is NOT the usual one.
    protected static final RhizomeApplicationServer ds       = new RhizomeApplicationServer(
            CassandraPod.class,
            DataManagerTestPod.class );
    protected static CassandraDataManager           cdm;

    protected static final Set<String>              PROFILES = Sets.newHashSet( "local", "cassandra" );

    @BeforeClass
    public static void init() {
        ds.sprout( PROFILES.toArray( new String[ 0 ] ) );
        cdm = ds.getContext().getBean( CassandraDataManager.class );
    }

    //@Test
    public void testWriteAndRead() {
        final UUID entitySetId = UUID.randomUUID();
        final UUID syncId = UUID.randomUUID();

        Map<UUID, CassandraPropertyReader> readers = generateProperties( 5 );
        Set<UUID> properties = readers.keySet();
        Map<UUID, EdmPrimitiveTypeKind> propertiesWithDataType = getDataTypeMapOfStringType( properties );
        Map<String, SetMultimap<UUID, Object>> entities = generateData( 10, properties, 1 );

        testWriteData( entitySetId, syncId, entities, propertiesWithDataType );
        Set<SetMultimap<FullQualifiedName, Object>> result = testReadData( ImmutableSet.of( syncId ),
                entitySetId,
                readers );

        Set<SetMultimap<FullQualifiedName, Object>> expected = convertGeneratedDataFromUuidToFqn( entities );
        Assert.assertEquals( expected, result );
    }

    @Test
    public void populateEmployeeCsv() throws FileNotFoundException, IOException {
        final UUID syncId = UUID.randomUUID();
        final UUID entitySetId = UUID.randomUUID();
        // Four property types: Employee Name, Title, Department, Salary
        Map<String, UUID> idLookup = getUUIDsForEmployeeCsvProperties();
        Map<UUID, CassandraPropertyReader> readers = getReadersForEmployeeCsv( idLookup );
        Map<UUID, EdmPrimitiveTypeKind> propertiesWithDataType = getDataTypeEmployeeCsv( idLookup );
        Set<UUID> properties = readers.keySet();

        try ( FileReader fr = new FileReader( "src/test/resources/employees.csv" );
                BufferedReader br = new BufferedReader( fr ) ) {

            String line;
            int count = 0;
            int paging_constant = 1000;
            Map<String, SetMultimap<UUID, Object>> entities = new HashMap<>();

            while ( ( line = br.readLine() ) != null ) {
                Employee employee = Employee.EmployeeCsvReader.getEmployee( line );
                System.out.println( employee.toString() );

                SetMultimap<UUID, Object> entity = HashMultimap.create();

                entity.put( idLookup.get( "name" ), employee.getName() );
                entity.put( idLookup.get( "title" ), employee.getTitle() );
                entity.put( idLookup.get( "dept" ), employee.getDept() );
                entity.put( idLookup.get( "salary" ), employee.getSalary() );

                if ( count++ < paging_constant ) {
                    entities.put( RandomStringUtils.randomAlphanumeric( 10 ), entity );
                } else {
                    cdm.createEntityData( entitySetId, syncId, entities, propertiesWithDataType );

                    entities = new HashMap<>();
                    count = 0;
                }
            }
        }

    }

    @AfterClass
    public static void dropKeyspace() {
        ds.getContext().getBean( "dropCassandraTestKeyspace", Runnable.class ).run();
    }

    public void testWriteData(
            UUID entitySetId,
            UUID syncId,
            Map<String, SetMultimap<UUID, Object>> entities,
            Map<UUID, EdmPrimitiveTypeKind> propertiesWithDataType ) {
        System.out.println( "Writing Data..." );
        cdm.createEntityData( entitySetId, syncId, entities, propertiesWithDataType );
        System.out.println( "Writing done." );
    }

    public Set<SetMultimap<FullQualifiedName, Object>> testReadData(
            Set<UUID> syncIds,
            UUID entitySetId,
            Map<UUID, CassandraPropertyReader> readers ) {
        return Sets.newHashSet( cdm.getEntitySetData( entitySetId, syncIds, readers ) );
    }

    private Map<UUID, CassandraPropertyReader> generateProperties( int n ) {
        System.out.println( "Generating Properties..." );
        Map<UUID, CassandraPropertyReader> readers = new HashMap<>();
        for ( int i = 0; i < n; i++ ) {
            UUID propertyId = UUID.randomUUID();
            readers.put( propertyId, getStringReader( propertyId ) );
        }
        System.out.println( "Properties generated." );
        return readers;
    }

    private Map<String, SetMultimap<UUID, Object>> generateData(
            int numOfEntities,
            Set<UUID> properties,
            int multiplicityOfProperties ) {
        System.out.println( "Generating data..." );

        final Map<String, SetMultimap<UUID, Object>> entities = new HashMap<>();
        for ( int i = 0; i < numOfEntities; i++ ) {
            String id = RandomStringUtils.randomAlphanumeric( 10 );
            SetMultimap<UUID, Object> propertyValues = HashMultimap.create();
            for ( UUID property : properties ) {
                for ( int k = 0; k < multiplicityOfProperties; k++ ) {
                    // Generate random numeric strings as value
                    String value = RandomStringUtils.randomNumeric( 5 );
                    propertyValues.put( property, value );
                    // For debugging
                    System.out.println( "Property: " + property + ", value generated: " + value );
                }
            }
            entities.put( id, propertyValues );
        }
        System.out.println( "Data generated." );
        return entities;
    }

    private Set<SetMultimap<FullQualifiedName, Object>> convertGeneratedDataFromUuidToFqn(
            Map<String, SetMultimap<UUID, Object>> map ) {
        Set<SetMultimap<FullQualifiedName, Object>> result = new HashSet<>();
        for ( SetMultimap<UUID, Object> v : map.values() ) {
            Map<FullQualifiedName, Collection<Object>> tmp = new HashMap<>();
            SetMultimap<FullQualifiedName, Object> ans = Multimaps.newSetMultimap( tmp, HashSet::new );
            v.asMap().entrySet().stream().forEach( e -> tmp.put( getFqnFromUuid( e.getKey() ), e.getValue() ) );
            result.add( ans );
        }
        return result;
    }

    /**
     * CassandraPropertyReader Utils
     */

    private CassandraPropertyReader getStringReader( UUID propertyId ) {
        return new CassandraPropertyReader( getFqnFromUuid( propertyId ), DataManagerTest::stdStringReader );
    }

    private FullQualifiedName getFqnFromUuid( UUID propertyId ) {
        return new FullQualifiedName( "test", propertyId.toString() );
    }

    private static String stdStringReader( Row row ) {
        return TypeCodec.varchar().deserialize( row.getBytes( "value" ), ProtocolVersion.NEWEST_SUPPORTED );
    }

    private CassandraPropertyReader getLongReader( UUID propertyId ) {
        return new CassandraPropertyReader( getFqnFromUuid( propertyId ), DataManagerTest::stdLongReader );
    }

    private static Long stdLongReader( Row row ) {
        return TypeCodec.counter().deserialize( row.getBytes( "value" ), ProtocolVersion.NEWEST_SUPPORTED );
    }

    private Map<UUID, CassandraPropertyReader> getReadersForEmployeeCsv( Map<String, UUID> idLookup ) {
        Map<UUID, CassandraPropertyReader> readers = new HashMap<>();
        readers.put( idLookup.get( "name" ), getStringReader( idLookup.get( "name" ) ) );
        readers.put( idLookup.get( "title" ), getStringReader( idLookup.get( "title" ) ) );
        readers.put( idLookup.get( "dept" ), getStringReader( idLookup.get( "dept" ) ) );
        readers.put( idLookup.get( "salary" ), getLongReader( idLookup.get( "salary" ) ) );
        return readers;
    }

    private Map<UUID, EdmPrimitiveTypeKind> getDataTypeEmployeeCsv( Map<String, UUID> idLookup ) {
        Map<UUID, EdmPrimitiveTypeKind> propertiesWithDataType = new HashMap<>();
        propertiesWithDataType.put( idLookup.get( "name" ), EdmPrimitiveTypeKind.String );
        propertiesWithDataType.put( idLookup.get( "title" ), EdmPrimitiveTypeKind.String );
        propertiesWithDataType.put( idLookup.get( "dept" ), EdmPrimitiveTypeKind.String );
        propertiesWithDataType.put( idLookup.get( "salary" ), EdmPrimitiveTypeKind.Int64 );
        return propertiesWithDataType;
    }

    private Map<String, UUID> getUUIDsForEmployeeCsvProperties() {
        Map<String, UUID> idLookup = new HashMap<>();
        idLookup.put( "name", UUID.randomUUID() );
        idLookup.put( "title", UUID.randomUUID() );
        idLookup.put( "dept", UUID.randomUUID() );
        idLookup.put( "salary", UUID.randomUUID() );
        return idLookup;
    }

    private Map<UUID, EdmPrimitiveTypeKind> getDataTypeMapOfStringType( Set<UUID> properties ) {
        return properties.stream().collect( Collectors.toMap( id -> id, id -> EdmPrimitiveTypeKind.String ) );
    }
}
