package com.dataloom.datastore.data;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dataloom.data.requests.EntitySetSelection;
import com.dataloom.datasource.UUIDs.Syncs;
import com.dataloom.datastore.authentication.MultipleAuthenticatedUsersBase;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;

public class DataControllerTest extends MultipleAuthenticatedUsersBase {

    private static final int    numberOfEntries = 10;
    private static final UUID   syncId          = Syncs.BASE.getSyncId();
    private static final Random random          = new Random();

    @BeforeClass
    public static void init() {
        loginAs( "admin" );
    }

    @Test
    public void testCreateAndLoadEntityData() {
        EntityType et = createEntityType();
        EntitySet es = createEntitySet( et );

        dataApi.createEntityData( es.getId(),
                syncId,
                TestDataFactory.randomStringEntityData( numberOfEntries, et.getProperties() ) );

        Iterable<SetMultimap<FullQualifiedName, Object>> results = dataApi.loadEntitySetData( es.getId(), null, "" );
        Assert.assertEquals( numberOfEntries, Iterables.size( results ) );
    }

    @Test
    public void testLoadSelectedEntityData() {
        EntityType et = createEntityType();
        EntitySet es = createEntitySet( et );

        Map<String, SetMultimap<UUID, Object>> entities = TestDataFactory.randomStringEntityData( numberOfEntries,
                et.getProperties() );
        dataApi.createEntityData( es.getId(), syncId, entities );

        // load selected data
        Set<UUID> selectedProperties = et.getProperties().stream().filter( pid -> random.nextBoolean() )
                .collect( Collectors.toSet() );
        EntitySetSelection ess = new EntitySetSelection(
                Optional.of( ImmutableSet.of( syncId ) ),
                Optional.of( selectedProperties ) );
        Iterable<SetMultimap<FullQualifiedName, Object>> results = dataApi.loadEntitySetData( es.getId(), ess, null );

        // check results
        // For each entity, collect its property value in one set, and collect all these sets together.
        Set<Set<String>> resultValues = new HashSet<>();
        for ( SetMultimap<FullQualifiedName, Object> entity : results ) {
            resultValues.add( entity.asMap().values().stream().flatMap( e -> e.stream() ).map( o -> (String) o )
                    .collect( Collectors.toSet() ) );
        }

        Set<Set<String>> expectedValues = new HashSet<>();
        for ( SetMultimap<UUID, Object> entity : entities.values() ) {
            expectedValues
                    .add( entity.asMap().entrySet().stream()
                            // filter the entries with key (propertyId) in the selected set
                            .filter( e -> selectedProperties.contains( e.getKey() ) )
                            // Put all the property values in the same stream, and cast them back to strings
                            .flatMap( e -> e.getValue().stream() )
                            .map( o -> (String) o )
                            .collect( Collectors.toSet() ) );
        }

        Assert.assertEquals( expectedValues, resultValues );
    }

    @Test
    public void testSyncTicketService() {
        EntityType et = createEntityType();
        EntitySet es = createEntitySet( et );

        UUID ticket = dataApi.acquireSyncTicket( es.getId(), syncId );

        dataApi.storeEntityData( ticket,
                syncId,
                TestDataFactory.randomStringEntityData( numberOfEntries, et.getProperties() ) );

        dataApi.releaseSyncTicket( ticket );

        // not passing in token should retain current security context
        Iterable<SetMultimap<FullQualifiedName, Object>> results = dataApi.loadEntitySetData( es.getId(), null, "" );
        Assert.assertEquals( numberOfEntries, Iterables.size( results ) );

    }
}
