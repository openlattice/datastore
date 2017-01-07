package com.kryptnostic.datastore.data;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dataloom.data.internal.Entity;
import com.dataloom.data.requests.CreateEntityRequest;
import com.datastax.driver.core.Row;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
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

    @Test
    public void testWriteAndRead() {
        final UUID syncId = UUID.randomUUID();
        final UUID entitySetId = UUID.randomUUID();

        Map<UUID, CassandraPropertyReader> readers = generateProperties( 5 );
        Set<UUID> properties = readers.keySet();
        CreateEntityRequest req = generateData( syncId, entitySetId, 10, properties, 1 );

        testWriteData( req, properties );
        Set<Entity> result = testReadData( ImmutableSet.of( syncId ), entitySetId, readers );

        Assert.assertEquals( req.getEntities(), result );
    }

    @Ignore
    public void populateEmployeeCsv() throws FileNotFoundException, IOException {
        final UUID syncId = UUID.randomUUID();
        final UUID entitySetId = UUID.randomUUID();
        // Four property types: Employee Name, Title, Department, Salary
        Map<String, UUID> idLookup = getUUIDsForEmployeeCsvProperties();
        Map<UUID, CassandraPropertyReader> readers = getReadersForEmployeeCsv( idLookup );
        Set<UUID> properties = readers.keySet();

        try ( FileReader fr = new FileReader( "src/test/resources/employees.csv" );
                BufferedReader br = new BufferedReader( fr ) ) {

            String line;
            int count = 0;
            int paging_constant = 100;
            Set<Entity> entities = new HashSet<>();

            while ( ( line = br.readLine() ) != null ) {
                Employee employee = Employee.EmployeeCsvReader.getEmployee( line );
                System.out.println( employee.toString() );

                SetMultimap<UUID, Object> entity = HashMultimap.create();

                entity.put( idLookup.get( "name" ), employee.getName() );
                entity.put( idLookup.get( "title" ), employee.getTitle() );
                entity.put( idLookup.get( "dept" ), employee.getDept() );
                entity.put( idLookup.get( "salary" ), employee.getSalary() );

                if ( count++ < paging_constant ) {
                    entities.add( new Entity( RandomStringUtils.randomAlphanumeric( 10 ), entity ) );
                } else {
                    CreateEntityRequest req = new CreateEntityRequest(
                            syncId,
                            entitySetId,
                            entities );
                    cdm.createEntityData( req, properties );

                    entities = new HashSet<>();
                    count = 0;
                }
            }
        }

    }

    @AfterClass
    public static void dropKeyspace() {
        ds.getContext().getBean( "dropCassandraTestKeyspace", Runnable.class ).run();
    }

    public void testWriteData( CreateEntityRequest req, Set<UUID> properties ) {
        System.out.println( "Writing Data..." );
        cdm.createEntityData( req, properties );
        System.out.println( "Writing done." );
    }

    public Set<Entity> testReadData( Set<UUID> syncIds, UUID entitySetId, Map<UUID, CassandraPropertyReader> readers ) {
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

    private CreateEntityRequest generateData(
            UUID syncId,
            UUID entitySetId,
            int numOfEntities,
            Set<UUID> properties,
            int multiplicityOfProperties ) {
        System.out.println( "Generating data..." );

        final Set<Entity> entities = new HashSet<>();
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
            entities.add( new Entity( id, propertyValues ) );
        }
        CreateEntityRequest req = new CreateEntityRequest( syncId, entitySetId, entities );
        System.out.println( "Data generated." );

        return req;
    }

    /**
     * CassandraPropertyReader Utils
     */

    private CassandraPropertyReader getStringReader( UUID propertyId ) {
        return new CassandraPropertyReader( propertyId, DataManagerTest::stdStringReader );
    }

    private static String stdStringReader( Row row ) {
        try {
            return (String) CassandraDataManager.deserialize( row.getBytes( "value" ) );
        } catch ( ClassNotFoundException | IOException e ) {
            System.err.println( "Error in deserializing Row " + row );
        }
        return null;
    }

    private CassandraPropertyReader getLongReader( UUID propertyId ) {
        return new CassandraPropertyReader( propertyId, DataManagerTest::stdLongReader );
    }

    private static Long stdLongReader( Row row ) {
        try {
            return (Long) CassandraDataManager.deserialize( row.getBytes( "value" ) );
        } catch ( ClassNotFoundException | IOException e ) {
            System.err.println( "Error in deserializing Row " + row );
        }
        return null;
    }

    private Map<UUID, CassandraPropertyReader> getReadersForEmployeeCsv( Map<String, UUID> idLookup ) {
        Map<UUID, CassandraPropertyReader> readers = new HashMap<>();
        readers.put( idLookup.get( "name" ), getStringReader( idLookup.get( "name" ) ) );
        readers.put( idLookup.get( "title" ), getStringReader( idLookup.get( "title" ) ) );
        readers.put( idLookup.get( "dept" ), getStringReader( idLookup.get( "dept" ) ) );
        readers.put( idLookup.get( "salary" ), getLongReader( idLookup.get( "salary" ) ) );
        return readers;
    }

    private Map<String, UUID> getUUIDsForEmployeeCsvProperties() {
        Map<String, UUID> idLookup = new HashMap<>();
        idLookup.put( "name", UUID.randomUUID() );
        idLookup.put( "title", UUID.randomUUID() );
        idLookup.put( "dept", UUID.randomUUID() );
        idLookup.put( "salary", UUID.randomUUID() );
        return idLookup;
    }

}
