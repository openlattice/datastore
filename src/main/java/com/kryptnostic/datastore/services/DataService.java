package com.kryptnostic.datastore.services;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.requests.CreateEntityRequest;
import com.dataloom.data.requests.LookupEntitiesRequest;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.Futures;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.conductor.rpc.ConductorCall;
import com.kryptnostic.conductor.rpc.Lambdas;
import com.kryptnostic.conductor.rpc.QueryResult;
import com.kryptnostic.conductor.rpc.ResultSetAdapterFactory;
import com.kryptnostic.conductor.rpc.UUIDs;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import com.kryptnostic.datastore.cassandra.CassandraStorage;
import com.kryptnostic.datastore.services.CassandraTableManager.PreparedStatementMapping;

public class DataService {

    private static final Logger          logger = LoggerFactory
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
        // tableManager.getTablenameForPropertyValues( )
        return null;
    }

    @Deprecated
    public Iterable<Multimap<FullQualifiedName, Object>> readAllEntitiesOfType( FullQualifiedName fqn ) {
        EntityType entityType = dms.getEntityType( fqn );
        return readAllEntitiesOfType( fqn, entityType.getProperties() );
    }

    public Iterable<Multimap<FullQualifiedName, Object>> readAllEntitiesOfType(
            FullQualifiedName fqn,
            Collection<FullQualifiedName> authorizedPropertyFqns ) {
        try {
            List<PropertyType> properties = authorizedPropertyFqns.stream()
                    .map( propertyTypeFqn -> dms.getPropertyType( propertyTypeFqn ) )
                    .collect( Collectors.toList() );

            QueryResult result = executor
                    .submit( ConductorCall
                            .wrap( Lambdas.getAllEntitiesOfType( fqn, properties ) ) )
                    .get();

            return Iterables.transform( result, row -> ResultSetAdapterFactory.mapRowToObject( row, properties ) );

        } catch ( InterruptedException | ExecutionException e ) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 
     * @param entityTypesAndProperties entity Type and collection of authorized properties for each type
     * @return
     */
    public Iterable<Iterable<Multimap<FullQualifiedName, Object>>> readAllEntitiesOfSchema(
            Map<FullQualifiedName, Collection<FullQualifiedName>> entityTypesAndProperties ) {
        // TODO This name is uber confusing...
        List<Iterable<Multimap<FullQualifiedName, Object>>> results = Lists.newArrayList();
        for ( Map.Entry<FullQualifiedName, Collection<FullQualifiedName>> entry : entityTypesAndProperties
                .entrySet() ) {
            readAllEntitiesOfType( entry.getKey(), entry.getValue() );
        }

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

    @Deprecated
    public void createEntityData( CreateEntityRequest createEntityRequest ) {
        FullQualifiedName entityFqn = createEntityRequest.getEntityType();
        Set<FullQualifiedName> propertyFqns = dms.getEntityType( entityFqn ).getProperties();

        createEntityData( createEntityRequest, propertyFqns );
    }

    public void createEntityData(
            CreateEntityRequest createEntityRequest,
            Set<FullQualifiedName> authorizedPropertyFqns ) {

        FullQualifiedName entityFqn = createEntityRequest.getEntityType();

        Map<FullQualifiedName, DataType> propertyDataTypeMap = authorizedPropertyFqns.stream()
                .collect( Collectors.toMap(
                        fqn -> fqn,
                        fqn -> CassandraEdmMapping
                                .getCassandraType( dms.getPropertyType( fqn ).getDatatype() ) ) );

        Set<SetMultimap<FullQualifiedName, Object>> propertyValues = createEntityRequest.getPropertyValues();
        UUID aclId = createEntityRequest.getAclId().or( UUIDs.ACLs.EVERYONE_ACL );
        UUID syncId = createEntityRequest.getSyncId().or( UUIDs.Syncs.BASE.getSyncId() );
        String typename = tableManager.getTypenameForEntityType( entityFqn );
        String entitySetName = createEntityRequest.getEntitySetName().orNull();
      
        PreparedStatementMapping cqm = tableManager.getInsertEntityPreparedStatement( entityFqn,
                authorizedPropertyFqns,
                createEntityRequest.getEntitySetName() );

        Object[] bindList =  new Object[ 4 + cqm.mapping.size() ];
        List<ResultSetFuture> results = propertyValues.stream().map( obj -> {
            UUID entityId = UUID.randomUUID();

            // TODO: This will keep the last value that appears ... i.e no property multiplicity.
            bindList[ 0 ] = entityId;
            bindList[ 1 ] = typename;
            bindList[ 2 ] = StringUtils.isBlank( entitySetName ) ? ImmutableSet.of() : ImmutableSet.of( entitySetName );
            bindList[ 3 ] = ImmutableList.of( syncId );

            obj.entries().stream().filter( e -> authorizedPropertyFqns.contains( e.getKey() ) ).forEach( e -> {
                DataType dt = propertyDataTypeMap.get( e.getKey() );
                Object propertyValue = CassandraEdmMapping.recoverJavaTypeFromCqlDataType( e.getValue(), dt );
                
                bindList[ cqm.mapping.get( e.getKey() ) ] = propertyValue;
            } );
            
            BoundStatement bq = cqm.stmt.bind( bindList );
            
            return session.executeAsync( bq );
        } ).collect( Collectors.toList() );
        
        try {
            Futures.allAsList( results ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Error writing data." ,  e );
        }
        /*
         * PreparedStatement createQuery = Preconditions.checkNotNull( tableManager.getInsertEntityPreparedStatement(
         * entityFqn ), "Insert data prepared statement does not exist." ); PreparedStatement
         * entityIdTypenameLookupQuery = Preconditions.checkNotNull(
         * tableManager.getUpdateEntityIdTypenamePreparedStatement( entityFqn ),
         * "Entity ID typename lookup query cannot be null." ); BoundStatement boundQuery = createQuery.bind( entityId,
         * typename, ImmutableSet.of( entitySetName ), ImmutableList.of( syncId ) ); logger.info(
         * "Attempting to create entity : {}", boundQuery.toString() ); session.execute( boundQuery ); session.execute(
         * entityIdTypenameLookupQuery.bind( typename, entityId ) ); EntityType entityType = dms.getEntityType(
         * entityFqn ); Set<FullQualifiedName> key = entityType.getKey(); // Pre-calculate properties that user can
         * actually write on Map<FullQualifiedName, Boolean> authorizationCheck = obj.keySet().stream() .collect(
         * Collectors.toMap( fqn -> fqn, fqn -> authorizedPropertyFqns.contains( fqn ) ) ); obj.entries().stream()
         * .filter( e -> authorizationCheck.get( e.getKey() ) ) .forEach( e -> { PreparedStatement pps =
         * tableManager.getUpdatePropertyPreparedStatement( e.getKey() ); logger.info(
         * "Attempting to write property value: {}", e.getValue() ); DataType dt = propertyDataTypeMap.get( e.getKey()
         * ); Object propertyValue = e.getValue(); if ( dt.equals( DataType.bigint() ) ) { propertyValue = Long.valueOf(
         * propertyValue.toString() ); } else if ( dt.equals( DataType.uuid() ) ) { // TODO Ho Chung: Added conversion
         * back to UUID; haven't checked other types propertyValue = UUID.fromString( propertyValue.toString() ); }
         * session.executeAsync( pps.bind( ImmutableList.of( syncId ), entityId, propertyValue ) ); if ( key.contains(
         * e.getKey() ) ) { PreparedStatement pipps = tableManager .getUpdatePropertyIndexPreparedStatement( e.getKey()
         * ); logger.info( "Attempting to write property Index: {}", e.getValue() ); session.executeAsync( pipps.bind(
         * ImmutableList.of( syncId ), propertyValue, entityId ) ); } } );
         */
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

    @Deprecated
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfEntitySet(
            String entitySetName,
            String entityTypeNamespace,
            String entityTypeName ) {
        EntityType entityType = dms.getEntityType( entityTypeNamespace, entityTypeName );
        Set<FullQualifiedName> propertyFqns = entityType.getProperties();

        return getAllEntitiesOfEntitySet( entitySetName, entityTypeNamespace, entityTypeName, propertyFqns );
    }

    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfEntitySet(
            String entitySetName,
            String entityTypeNamespace,
            String entityTypeName,
            Collection<FullQualifiedName> authorizedPropertyFqns ) {
        Iterable<Multimap<FullQualifiedName, Object>> result = Lists.newArrayList();
        FullQualifiedName entityTypeFqn = new FullQualifiedName( entityTypeNamespace, entityTypeName );
        try {
            List<PropertyType> properties = authorizedPropertyFqns.stream()
                    .map( propertyTypeFqn -> dms.getPropertyType( propertyTypeFqn ) )
                    .collect( Collectors.toList() );

            QueryResult qr = executor
                    .submit( ConductorCall
                            .wrap( Lambdas.getAllEntitiesOfEntitySet( entityTypeFqn, entitySetName, properties ) ) )
                    .get();

            result = Iterables.transform( qr, row -> ResultSetAdapterFactory.mapRowToObject( row, properties ) );
            return result;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( e.getMessage() );
        }
        return null;
    }
}
