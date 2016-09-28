package com.kryptnostic.datastore.services;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.kryptnostic.conductor.rpc.*;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.cassandra.CassandraStorage;

public class DataService {
    private static final Logger logger = LoggerFactory
            .getLogger( DataService.class );
    // private final IMap<String, FullQualifiedName> entitySets;
    // private final IMap<FullQualifiedName, EntitySchema> entitySchemas;
    private final EdmManager             dms;
    private final CassandraTableManager  tableManager;
    private final Session                session;
    private final String                 keyspace;
    private final DurableExecutorService executor;
    private final IMap<String, String>   urlToIntegrationScripts;

    public DataService(
            String keyspace,
            HazelcastInstance hazelcast,
            EdmManager dms,
            Session session,
            CassandraTableManager tableManager,
            CassandraStorage storage,
            MappingManager mm ) {
        this.dms = dms;
        this.tableManager = tableManager;
        this.session = session;
        this.keyspace = keyspace;
        // TODO: Configure executor service.
        this.executor = hazelcast.getDurableExecutorService( "default" );
        this.urlToIntegrationScripts = hazelcast.getMap( "url_to_scripts" );
    }

    public Map<String, Object> getObject( UUID objectId ) {
        //        tableManager.getTablenameForPropertyValues( )
        return null;
    }

    public Iterable<Multimap<FullQualifiedName, Object>> readAllEntitiesOfType( FullQualifiedName fqn ) {
        try {
            QueryResult result = executor
                    .submit( ConductorCall
                            .wrap( Lambdas.getAllEntitiesOfType( fqn ) ) )
                    .get();
            //Get properties of the entityType
            EntityType entityType = dms.getEntityType( fqn );
            Set<PropertyType> properties = new HashSet<PropertyType>();
            entityType.getProperties().forEach(
                    property -> properties.add( dms.getPropertyType( property ) )
            );

            return Iterables.transform( result, row -> ResultSetAdapterFactory.mapRowToObject( row, properties ) );

        } catch ( InterruptedException | ExecutionException e ) {
            e.printStackTrace();
        }
        return null;
    }

    public Iterable<Iterable<Multimap<FullQualifiedName, Object>>> readAllEntitiesOfSchema( List<FullQualifiedName> fqns ){

        List<Iterable<Multimap<FullQualifiedName, Object>>> results = Lists.newArrayList();
        fqns.forEach( fqn -> {
            try {
                QueryResult result = executor.submit( ConductorCall
                .wrap( Lambdas.getAllEntitiesOfType( fqn ) )).get();

                EntityType entityType = dms.getEntityType( fqn );
                Set<PropertyType> properties = entityType.getProperties().stream().map(
                        property ->  dms.getPropertyType( property ) ).collect( Collectors.toSet());

                results.add( Iterables.transform( result, row -> ResultSetAdapterFactory.mapRowToObject( row, properties ) ) );
            } catch ( InterruptedException | ExecutionException e ) {
                e.printStackTrace();
            }
        } );

        return results;
    }

    public Iterable<UUID> loadEntitySetOfType( FullQualifiedName fqn ) {
        try {
            Iterable<UUID> result = executor
                    .submit( ConductorCall
                            .wrap( Lambdas.getEntitySetOfType( fqn ) ) )
                    .get();
            return result;
        } catch ( InterruptedException | ExecutionException e ) {
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, String> getAllIntegrationScripts() {
        return urlToIntegrationScripts;
    }

    public Map<String, String> getIntegrationScriptForUrl( Set<String> urls ) {
        return urlToIntegrationScripts.getAll( urls );
    }

    public void createIntegrationScript( Map<String, String> integrationScripts ) {
        urlToIntegrationScripts.putAll( integrationScripts );
    }

    public void createEntityData( CreateEntityRequest createEntityRequest ) {

        FullQualifiedName entityFqn = createEntityRequest.getEntityType();
        Set<FullQualifiedName> propertyFqns = dms.getEntityType( entityFqn ).getProperties();
        Set<Multimap<FullQualifiedName, Object>> propertyValues = createEntityRequest.getPropertyValues();
        UUID aclId = createEntityRequest.getAclId().or( UUIDs.ACLs.EVERYONE_ACL );
        UUID syncId = createEntityRequest.getSyncId().or( UUIDs.Syncs.BASE.getSyncId() );

        propertyValues.stream().forEach( obj -> {
            PreparedStatement createQuery = Preconditions.checkNotNull(
                    tableManager.getInsertEntityPreparedStatement( entityFqn ),
                    "Insert data prepared statement does not exist." );

            PreparedStatement entityIdTypenameLookupQuery = Preconditions.checkNotNull(
                    tableManager.getUpdateEntityIdTypenamePreparedStatement( entityFqn ),
                    "Entity ID typename lookup query cannot be null." );

            UUID entityId = UUID.randomUUID();
            String typename = tableManager.getTypenameForEntityType( entityFqn );
            BoundStatement boundQuery = createQuery.bind( entityId,
                    typename,
                    ImmutableSet.of( createEntityRequest.getEntitySetName() ),
                    ImmutableList.of( syncId ) );
            logger.info( "Attempting to create entity : {}", boundQuery.toString() );
            session.execute( boundQuery );
            session.execute( entityIdTypenameLookupQuery.bind( typename, entityId ) );

            EntityType entityType = dms.getEntityType( entityFqn );
            Map<FullQualifiedName, DataType> propertyDataTypeMap = propertyFqns.stream().collect( Collectors.toMap(
                    fqn -> fqn,
                    fqn -> CassandraEdmMapping.getCassandraType( dms.getPropertyType( fqn ).getDatatype() ) ) );

            Set<FullQualifiedName> key = entityType.getKey();
            obj.entries().stream().forEach( e -> {
                PreparedStatement pps = tableManager.getUpdatePropertyPreparedStatement( e.getKey() );

                logger.info( "Attempting to write property value: {}", e.getValue() );
                DataType dt = propertyDataTypeMap.get( e.getKey() );
                Object propertyValue = e.getValue();
                if ( dt.equals( DataType.bigint() ) ) {
                    propertyValue = Long.valueOf( propertyValue.toString() );
                }
                session.executeAsync( pps.bind( ImmutableList.of( syncId ), entityId, propertyValue ) );
                if ( key.contains( e.getKey() ) ) {
                    PreparedStatement pipps = tableManager.getUpdatePropertyIndexPreparedStatement( e.getKey() );
                    logger.info( "Attempting to write property Index: {}", e.getValue() );
                    session.executeAsync( pipps.bind( ImmutableList.of( syncId ), propertyValue, entityId ) );
                }
            } );
        } );
    }

	public Iterable<UUID> getFilteredEntities(
			LookupEntitiesRequest lookupEntitiesRequest) {
        try {
            QueryResult result = executor
                    .submit( ConductorCall
                            .wrap( Lambdas.getFilteredEntities ( lookupEntitiesRequest ) ) )
                    .get();
            return Iterables.transform( result, row -> ResultSetAdapterFactory.mapRowToUUID( row ) );

        } catch ( InterruptedException | ExecutionException e ) {
            e.printStackTrace();
        }
        return null;
	}

    public Iterable<Multimap<FullQualifiedName,Object>> getAllEntitiesOfEntitySet( String entitySetName, String entityTypeName ) {
        Iterable<Multimap<FullQualifiedName, Object>> result = Lists.newArrayList();
        FullQualifiedName fqn = tableManager.getEntityTypeForTypename( entityTypeName );
        try{
            QueryResult qr = executor
                    .submit( ConductorCall.wrap( Lambdas.getAllEntitiesOfEntitySet( fqn, entitySetName ) ) ).get();
            EntityType entityType = dms.getEntityType( fqn );
            Set<PropertyType> properties = entityType.getProperties().stream().map(
                    property ->  dms.getPropertyType( property ) ).collect( Collectors.toSet());

            result = Iterables.transform( qr, row -> ResultSetAdapterFactory.mapRowToObject( row, properties ) );
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( e.getMessage() );
        }
        return result;
    }
}
