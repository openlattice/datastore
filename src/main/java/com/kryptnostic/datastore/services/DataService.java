package com.kryptnostic.datastore.services;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.conductor.rpc.ConductorCall;
import com.kryptnostic.conductor.rpc.Lambdas;
import com.kryptnostic.conductor.rpc.LookupEntitiesRequest;
import com.kryptnostic.conductor.rpc.QueryResult;
import com.kryptnostic.conductor.rpc.ResultSetAdapterFactory;
import com.kryptnostic.conductor.rpc.UUIDs;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import com.kryptnostic.datastore.cassandra.CassandraStorage;
import com.kryptnostic.datastore.services.requests.CreateEntityRequest;

public class DataService {

    private static final Logger              logger = LoggerFactory
            .getLogger( DataService.class );
    // private final IMap<String, FullQualifiedName> entitySets;
    // private final IMap<FullQualifiedName, EntitySchema> entitySchemas;
    private final EdmManager                 dms;
    private final CassandraTableManager      tableManager;
    private final Session                    session;
    private final String                     keyspace;
    private final DurableExecutorService     executor;
    private final IMap<String, String>       urlToIntegrationScripts;
    private final ActionAuthorizationService authzService;

    public DataService(
            String keyspace,
            HazelcastInstance hazelcast,
            EdmManager dms,
            Session session,
            CassandraTableManager tableManager,
            CassandraStorage storage,
            MappingManager mm,
            ActionAuthorizationService authzService ) {
        this.dms = dms;
        this.tableManager = tableManager;
        this.session = session;
        this.keyspace = keyspace;
        // TODO: Configure executor service.
        this.executor = hazelcast.getDurableExecutorService( "default" );
        this.urlToIntegrationScripts = hazelcast.getMap( "url_to_scripts" );
        this.authzService = authzService;
    }
    
    public Map<String, Object> getObject( UUID objectId ) {
        // tableManager.getTablenameForPropertyValues( )
        return null;
    }

    public Iterable<Multimap<FullQualifiedName, Object>> readAllEntitiesOfType( FullQualifiedName fqn ) {
        if ( authzService.readAllEntitiesOfType( fqn ) ) {
            try {
                QueryResult result = executor
                        .submit( ConductorCall
                                .wrap( Lambdas.getAllEntitiesOfType( fqn ) ) )
                        .get();
                // Get properties of the entityType
                EntityType entityType = dms.getEntityType( fqn );
                Set<PropertyType> properties = entityType.getProperties().stream()
                        .filter( propertyTypeFqn -> authzService.readPropertyTypeInEntityType( fqn, propertyTypeFqn ) )
                        .map( propertyTypeFqn -> dms.getPropertyType( propertyTypeFqn ) )
                        .collect( Collectors.toSet() );

                return Iterables.transform( result, row -> ResultSetAdapterFactory.mapRowToObject( row, properties ) );

            } catch ( InterruptedException | ExecutionException e ) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public Iterable<Iterable<Multimap<FullQualifiedName, Object>>> readAllEntitiesOfSchema(
            List<FullQualifiedName> fqns ) {
        // TODO This name is uber confusing...
        List<Iterable<Multimap<FullQualifiedName, Object>>> results = Lists.newArrayList();
        fqns.forEach( fqn -> {
            results.add( readAllEntitiesOfType( fqn ) );
        } );

        return results;
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

        boolean authorizedToWrite = false;

        FullQualifiedName entityFqn = createEntityRequest.getEntityType();
        if ( createEntityRequest.getEntitySetName().isPresent() ) {
            authorizedToWrite = authzService.createEntityOfEntitySet( entityFqn,
                    createEntityRequest.getEntitySetName().get() );
        } else {
            authorizedToWrite = authzService.createEntityOfEntityType( entityFqn );
        }

        if ( authorizedToWrite ) {
            Set<FullQualifiedName> propertyFqns = dms.getEntityType( entityFqn ).getProperties();

            // Check properties that user has permissions to write on.
            // Unauthorized properties will be skipped
            Map<FullQualifiedName, Boolean> authorizationForPropertyWrite;
            Map<FullQualifiedName, DataType> propertyDataTypeMap;
            if ( createEntityRequest.getEntitySetName().isPresent() ) {
                authorizationForPropertyWrite = propertyFqns.stream().collect( Collectors.toMap(
                        propertyTypeFqn -> propertyTypeFqn,
                        propertyTypeFqn -> authzService.writePropertyTypeInEntitySet( entityFqn,
                                createEntityRequest.getEntitySetName().get(),
                                propertyTypeFqn ) ) );

                propertyDataTypeMap = propertyFqns.stream()
                        .collect( Collectors.toMap(
                                fqn -> fqn,
                                fqn -> CassandraEdmMapping
                                        .getCassandraType( dms.getPropertyType( fqn ).getDatatype() ) ) );
            } else {
                authorizationForPropertyWrite = propertyFqns.stream().collect( Collectors.toMap(
                        propertyTypeFqn -> propertyTypeFqn,
                        propertyTypeFqn -> authzService.writePropertyTypeInEntityType( entityFqn, propertyTypeFqn ) ) );

                propertyDataTypeMap = propertyFqns.stream()
                        .collect( Collectors.toMap(
                                fqn -> fqn,
                                fqn -> CassandraEdmMapping
                                        .getCassandraType( dms.getPropertyType( fqn ).getDatatype() ) ) );
            }

            Set<Multimap<FullQualifiedName, Object>> propertyValues = createEntityRequest.getPropertyValues();
            UUID aclId = createEntityRequest.getAclId().or( UUIDs.ACLs.EVERYONE_ACL );
            UUID syncId = createEntityRequest.getSyncId().or( UUIDs.Syncs.BASE.getSyncId() );
            String typename = tableManager.getTypenameForEntityType( entityFqn );
            String entitySetName = createEntityRequest.getEntitySetName()
                    .or( CassandraTableManager.getNameForDefaultEntitySet( typename ) );

            propertyValues.stream().forEach( obj -> {
                PreparedStatement createQuery = Preconditions.checkNotNull(
                        tableManager.getInsertEntityPreparedStatement( entityFqn ),
                        "Insert data prepared statement does not exist." );

                PreparedStatement entityIdTypenameLookupQuery = Preconditions.checkNotNull(
                        tableManager.getUpdateEntityIdTypenamePreparedStatement( entityFqn ),
                        "Entity ID typename lookup query cannot be null." );

                UUID entityId = UUID.randomUUID();
                BoundStatement boundQuery = createQuery.bind( entityId,
                        typename,
                        ImmutableSet.of( entitySetName ),
                        ImmutableList.of( syncId ) );
                logger.info( "Attempting to create entity : {}", boundQuery.toString() );
                session.execute( boundQuery );
                session.execute( entityIdTypenameLookupQuery.bind( typename, entityId ) );

                EntityType entityType = dms.getEntityType( entityFqn );

                Set<FullQualifiedName> key = entityType.getKey();
                obj.entries().stream()
                        .filter( e -> authorizationForPropertyWrite.get( e.getKey() ) )
                        .forEach( e -> {
                            PreparedStatement pps = tableManager.getUpdatePropertyPreparedStatement( e.getKey() );

                            logger.info( "Attempting to write property value: {}", e.getValue() );
                            DataType dt = propertyDataTypeMap.get( e.getKey() );
                            
                            Object propertyValue = e.getValue();
                            if ( dt.equals( DataType.bigint() ) ) {
                                propertyValue = Long.valueOf( propertyValue.toString() );
                            } else if (dt.equals(  DataType.uuid() ) ){
                                //TODO Ho Chung: Added conversion back to UUID; haven't checked other types
                                propertyValue = UUID.fromString( propertyValue.toString() );
                            }
                            session.executeAsync( pps.bind( ImmutableList.of( syncId ), entityId, propertyValue ) );
                            if ( key.contains( e.getKey() ) ) {
                                PreparedStatement pipps = tableManager
                                        .getUpdatePropertyIndexPreparedStatement( e.getKey() );
                                logger.info( "Attempting to write property Index: {}", e.getValue() );
                                session.executeAsync(
                                        pipps.bind( ImmutableList.of( syncId ), propertyValue, entityId ) );
                            }
                        } );
            } );
        }
    }

    // TODO Permissions stuff not added yet - would need to modify a bit. Don't think frontend has been using it, so am
    // skipping it for now.
    public Iterable<UUID> getFilteredEntities(
            LookupEntitiesRequest lookupEntitiesRequest ) {
        try {
            QueryResult result = executor
                    .submit( ConductorCall
                            .wrap( Lambdas.getFilteredEntities( lookupEntitiesRequest ) ) )
                    .get();
            return Iterables.transform( result, row -> ResultSetAdapterFactory.mapRowToUUID( row ) );

        } catch ( InterruptedException | ExecutionException e ) {
            e.printStackTrace();
        }
        return null;
    }

    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfEntitySet(
            String entitySetName,
            String entityTypeNamespace,
            String entityTypeName ) {
        Iterable<Multimap<FullQualifiedName, Object>> result = Lists.newArrayList();
        FullQualifiedName entityTypeFqn = new FullQualifiedName( entityTypeNamespace, entityTypeName );
        if ( authzService.getAllEntitiesOfEntitySet( entityTypeFqn, entitySetName ) ) {
            try {
                QueryResult qr = executor
                        .submit( ConductorCall
                                .wrap( Lambdas.getAllEntitiesOfEntitySet( entityTypeFqn, entitySetName ) ) )
                        .get();

                EntityType entityType = dms.getEntityType( entityTypeFqn );
                // Need to edit this to viewable properties
                Set<PropertyType> properties = entityType.getProperties().stream()
                        .filter( propertyTypeFqn -> authzService.readPropertyTypeInEntitySet( entityTypeFqn,
                                entitySetName,
                                propertyTypeFqn ) )
                        .map( propertyTypeFqn -> dms.getPropertyType( propertyTypeFqn ) )
                        .collect( Collectors.toSet() );

                result = Iterables.transform( qr, row -> ResultSetAdapterFactory.mapRowToObject( row, properties ) );
            } catch ( InterruptedException | ExecutionException e ) {
                logger.error( e.getMessage() );
            }
            return result;
        }
        return null;
    }
}
