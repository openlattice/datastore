/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.datastore.data;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dataloom.datastore.BootstrapDatastoreWithCassandra;
import com.dataloom.edm.type.PropertyType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.kryptnostic.conductor.rpc.Employee;

public class DataManagerTest extends BootstrapDatastoreWithCassandra {
    protected static final Set<String>              PROFILES = Sets.newHashSet( "local", "cassandra" );

    private static final List<EdmPrimitiveTypeKind> edmTypesList;
    private static final int                        edmTypesSize;

    private static final Random                     random   = new Random();
    private static ObjectMapper                     mapper;

    static {
        edmTypesList = Arrays.asList(
                EdmPrimitiveTypeKind.Binary,
                EdmPrimitiveTypeKind.Boolean,
                EdmPrimitiveTypeKind.Byte,
                EdmPrimitiveTypeKind.SByte,
                EdmPrimitiveTypeKind.Date,
                EdmPrimitiveTypeKind.DateTimeOffset,
                EdmPrimitiveTypeKind.TimeOfDay,
                EdmPrimitiveTypeKind.Duration,
                EdmPrimitiveTypeKind.Decimal,
                EdmPrimitiveTypeKind.Single,
                EdmPrimitiveTypeKind.Double,
                EdmPrimitiveTypeKind.Guid,
                EdmPrimitiveTypeKind.Int16,
                EdmPrimitiveTypeKind.Int32,
                EdmPrimitiveTypeKind.Int64,
                EdmPrimitiveTypeKind.String,
                EdmPrimitiveTypeKind.GeographyPoint );
        edmTypesSize = edmTypesList.size();
    }

    @BeforeClass
    public static void initDMTest() {
//        if ( initLock.readLock().tryLock() ) {
            mapper = ds.getContext().getBean( ObjectMapper.class );
//        }
//        initLock.readLock().unlock();
    }

    @Test
    public void testWriteAndRead() {
        final UUID entitySetId = UUID.randomUUID();
        final UUID syncId = UUID.randomUUID();

        Map<UUID, PropertyType> propertyTypes = generateProperties( 5 );
        Map<UUID, EdmPrimitiveTypeKind> propertiesWithDataType = propertyTypes.entrySet().stream()
                .collect( Collectors.toMap( e -> e.getKey(), e -> e.getValue().getDatatype() ) );
        Map<String, SetMultimap<UUID, Object>> entities = generateData( 10, propertiesWithDataType, 1 );

        testWriteData( entitySetId, syncId, entities, propertiesWithDataType );
        Set<SetMultimap<FullQualifiedName, Object>> result = testReadData( ImmutableSet.of( syncId ),
                entitySetId,
                propertyTypes );

        Set<SetMultimap<FullQualifiedName, Object>> expected = convertGeneratedDataFromUuidToFqn( entities );

        Assert.assertEquals( convertValueToString( expected ), convertValueToString( result ) );
    }
    
    @Test
    public void testWriteAndDelete() {
        final UUID entitySetId = UUID.randomUUID();
        final UUID firstSyncId = UUID.randomUUID();
        final UUID secondSyncId = UUID.randomUUID();

        Map<UUID, PropertyType> propertyTypes = generateProperties( 5 );
        Map<UUID, EdmPrimitiveTypeKind> propertiesWithDataType = propertyTypes.entrySet().stream()
                .collect( Collectors.toMap( e -> e.getKey(), e -> e.getValue().getDatatype() ) );
        
        Map<String, SetMultimap<UUID, Object>> firstEntities = generateData( 10, propertiesWithDataType, 1 );
        testWriteData( entitySetId, firstSyncId, firstEntities, propertiesWithDataType );

        Map<String, SetMultimap<UUID, Object>> secondEntities = generateData( 10, propertiesWithDataType, 1 );
        testWriteData( entitySetId, secondSyncId, secondEntities, propertiesWithDataType );

        dataService.deleteEntitySetData( entitySetId );
        
        Assert.assertEquals( 0, testReadData( ImmutableSet.of( firstSyncId, secondSyncId ),
                entitySetId,
                propertyTypes ).size() );
    }

    @Ignore
    public void populateEmployeeCsv() throws FileNotFoundException, IOException {
        final UUID syncId = UUID.randomUUID();
        final UUID entitySetId = UUID.randomUUID();
        // Four property types: Employee Name, Title, Department, Salary
        Map<String, UUID> idLookup = getUUIDsForEmployeeCsvProperties();
        Map<UUID, PropertyType> propertyTypes = getPropertiesForEmployeeCsv( idLookup );
        Map<UUID, EdmPrimitiveTypeKind> propertiesWithDataType = propertyTypes.entrySet().stream()
                .collect( Collectors.toMap( e -> e.getKey(), e -> e.getValue().getDatatype() ) );

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
                    dataService.createEntityData( entitySetId, syncId, entities, propertiesWithDataType );

                    entities = new HashMap<>();
                    count = 0;
                }
            }
        }

    }

    public void testWriteData(
            UUID entitySetId,
            UUID syncId,
            Map<String, SetMultimap<UUID, Object>> entities,
            Map<UUID, EdmPrimitiveTypeKind> propertiesWithDataType ) {
        System.out.println( "Writing Data..." );
        dataService.createEntityData( entitySetId, syncId, entities, propertiesWithDataType );
        System.out.println( "Writing done." );
    }

    public Set<SetMultimap<FullQualifiedName, Object>> testReadData(
            Set<UUID> syncIds,
            UUID entitySetId,
            Map<UUID, PropertyType> propertyTypes ) {
        return Sets.newHashSet( dataService.getEntitySetData( entitySetId, syncIds, propertyTypes ) );
    }

    private Map<UUID, PropertyType> generateProperties( int n ) {
        System.out.println( "Generating Properties..." );
        Map<UUID, PropertyType> propertyTypes = new HashMap<>();
        for ( int i = 0; i < n; i++ ) {
            UUID propertyId = UUID.randomUUID();
            propertyTypes.put( propertyId, getRandomPropertyType( propertyId ) );
        }
        System.out.println( "Properties generated." );
        return propertyTypes;
    }

    private Map<String, SetMultimap<UUID, Object>> generateData(
            int numOfEntities,
            Map<UUID, EdmPrimitiveTypeKind> propertiesWithDataType,
            int multiplicityOfProperties ) {
        System.out.println( "Generating data..." );

        final Map<String, SetMultimap<UUID, Object>> entities = new HashMap<>();
        Set<UUID> properties = propertiesWithDataType.keySet();
        for ( int i = 0; i < numOfEntities; i++ ) {
            String id = RandomStringUtils.randomAlphanumeric( 10 );
            SetMultimap<UUID, Object> propertyValues = HashMultimap.create();
            for ( UUID property : properties ) {
                for ( int k = 0; k < multiplicityOfProperties; k++ ) {
                    // Generate random numeric strings as value
                    Object value = getRandomValue( propertiesWithDataType.get( property ) );
                    propertyValues.put( property, value );
                    // For debugging
                    System.out.println( "Property: " + property + ", type: " + propertiesWithDataType.get( property )
                            + ", value generated: " + value );
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
            SetMultimap<FullQualifiedName, Object> ans = HashMultimap.create();
            v.entries().stream().forEach( e -> ans.put( getFqnFromUuid( e.getKey() ), e.getValue() ) );
            result.add( ans );
        }
        return result;
    }

    /**
     * Utils
     */

    private FullQualifiedName getFqnFromUuid( UUID propertyId ) {
        return new FullQualifiedName( "test", propertyId.toString() );
    }

    private Map<UUID, PropertyType> getPropertiesForEmployeeCsv( Map<String, UUID> idLookup ) {
        Map<UUID, PropertyType> propertyTypes = new HashMap<>();
        PropertyType name = new PropertyType(
                idLookup.get( "name" ),
                getFqnFromUuid( idLookup.get( "name" ) ),
                "Name",
                Optional.of( "Employee Name" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String );
        PropertyType title = new PropertyType(
                idLookup.get( "title" ),
                getFqnFromUuid( idLookup.get( "title" ) ),
                "Title",
                Optional.of( "Employee Title" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String );
        PropertyType dept = new PropertyType(
                idLookup.get( "dept" ),
                getFqnFromUuid( idLookup.get( "dept" ) ),
                "Dept",
                Optional.of( "Employee Department" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String );
        PropertyType salary = new PropertyType(
                idLookup.get( "salary" ),
                getFqnFromUuid( idLookup.get( "salary" ) ),
                "Salary",
                Optional.of( "Employee Salary" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.Int64 );

        propertyTypes.put( idLookup.get( "name" ), name );
        propertyTypes.put( idLookup.get( "title" ), title );
        propertyTypes.put( idLookup.get( "dept" ), dept );
        propertyTypes.put( idLookup.get( "salary" ), salary );
        return propertyTypes;
    }

    private Map<String, UUID> getUUIDsForEmployeeCsvProperties() {
        Map<String, UUID> idLookup = new HashMap<>();
        idLookup.put( "name", UUID.randomUUID() );
        idLookup.put( "title", UUID.randomUUID() );
        idLookup.put( "dept", UUID.randomUUID() );
        idLookup.put( "salary", UUID.randomUUID() );
        return idLookup;
    }

    private PropertyType getRandomPropertyType( UUID id ) {
        EdmPrimitiveTypeKind type = getRandomEdmType();
        return new PropertyType(
                id,
                getFqnFromUuid( id ),
                "Property " + id.toString(),
                Optional.absent(),
                ImmutableSet.of(),
                type );

    }

    private EdmPrimitiveTypeKind getRandomEdmType() {
        return edmTypesList.get( random.nextInt( edmTypesSize ) );
    }

    /**
     * See
     * http://docs.oasis-open.org/odata/odata-json-format/v4.0/errata03/os/odata-json-format-v4.0-errata03-os-complete.html#_Toc453766642
     */
    @SuppressWarnings( "unchecked" )
    private Object getRandomValue( EdmPrimitiveTypeKind type ) {
        Object rawObj;
        switch ( type ) {
            case Binary:
                rawObj = RandomStringUtils.randomAlphanumeric( 10 );
                break;
            case Boolean:
                rawObj = random.nextBoolean();
                break;
            case Byte:
                rawObj = random.nextInt( 8 );
                break;
            case Date:
                rawObj = ( 1000 + random.nextInt( 2000 ) ) + "-" + ( 1 + random.nextInt( 12 ) ) + "-"
                        + ( 1 + random.nextInt( 28 ) );
                break;
            case DateTimeOffset:
                rawObj = DateTime.now().toString();
                break;
            case Decimal:
                rawObj = new BigDecimal( Math.random() );
                break;
            case Double:
                rawObj = random.nextDouble();
                break;
            case Duration:
                rawObj = random.nextInt() + "d" + random.nextInt( 24 ) + "h" + random.nextInt( 60 ) + "m"
                        + random.nextInt( 60 )
                        + "m";
                break;
            case Guid:
                rawObj = UUID.randomUUID();
                break;
            case Int16:
                rawObj = (short) random.nextInt( Short.MAX_VALUE + 1 );
                break;
            case Int32:
                rawObj = random.nextInt();
                break;
            case Int64:
                rawObj = random.nextLong();
                break;
            case String:
                rawObj = RandomStringUtils.randomAlphanumeric( 10 );
                break;
            case SByte:
                rawObj = random.nextInt( 256 ) - 128;
                break;
            case Single:
                rawObj = random.nextFloat();
                break;
            case TimeOfDay:
                rawObj = random.nextInt( 24 ) + ":" + random.nextInt( 60 ) + ":" + random.nextInt( 60 ) + "."
                        + random.nextInt( 1000 );
                break;
            case GeographyPoint:
                // Should follow GeoJson format as in https://tools.ietf.org/html/draft-butler-geojson-04#appendix-A.1
                rawObj = new HashMap<String, Object>();
                ( (Map<String, Object>) rawObj ).put( "type", "Point" );
                ( (Map<String, Object>) rawObj ).put( "coordinates",
                        Arrays.asList( random.nextDouble(), random.nextDouble() ) );
                break;
            default:
                rawObj = null;
        }
        try {
            String value = mapper.writeValueAsString( rawObj );
            return mapper.readValue( value, Object.class );
        } catch ( IOException e ) {
            e.printStackTrace();
            return null;
        }
    }

    private Set<SetMultimap<FullQualifiedName, String>> convertValueToString(
            Set<SetMultimap<FullQualifiedName, Object>> set ) {
        Set<SetMultimap<FullQualifiedName, String>> result = new HashSet<>();
        for ( SetMultimap<FullQualifiedName, Object> map : set ) {
            SetMultimap<FullQualifiedName, String> ans = HashMultimap.create();
            map.entries().stream().forEach( e -> ans.put( e.getKey(), e.getValue().toString() ) );
            result.add( ans );
        }
        return result;
    }
}
