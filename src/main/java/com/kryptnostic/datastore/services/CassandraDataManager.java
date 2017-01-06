package com.kryptnostic.datastore.services;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.internal.Entity;
import com.dataloom.data.requests.CreateEntityRequest;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.Futures;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CassandraPropertyReader;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;
import com.kryptnostic.rhizome.cassandra.ColumnDef;

public class CassandraDataManager {
    private static final Logger     logger       = LoggerFactory
            .getLogger( CassandraDataManager.class );
    private static ColumnDef        VALUE_COLUMN = new CassandraTableBuilder.ValueColumn(
            "value",
            DataType.blob() );
    private final String            keyspace;
    private final Session           session;
    private final PreparedStatement writeIdLookupQuery;
    private final PreparedStatement writeDataQuery;
    private final PreparedStatement entitySetQuery;
    private final PreparedStatement entityIdsQuery;

    public CassandraDataManager( String keyspace, Session session ) {
        this.keyspace = keyspace;
        this.session = session;
        CassandraTableBuilder idLookupTableDefinitions = defineIdLookupTables( keyspace );
        CassandraTableBuilder dataTableDefinitions = defineDataTables( keyspace );
        prepareTables( session, idLookupTableDefinitions, dataTableDefinitions );

        this.entitySetQuery = prepareEntitySetQuery( session, dataTableDefinitions );
        this.entityIdsQuery = prepareEntityIdsQuery( keyspace, session );
        this.writeIdLookupQuery = prepareWriteIdLookupQuery( session, dataTableDefinitions );
        this.writeDataQuery = prepareWriteDataQuery( session, idLookupTableDefinitions );
    }

    public Iterable<SetMultimap<UUID, Object>> getEntitySetData(
            UUID entitySetId,
            Set<UUID> syncIds,
            Map<UUID, CassandraPropertyReader> authorizedPropertyTypes ) {
        Set<UUID> authorizedProperties = authorizedPropertyTypes.keySet();
        Iterable<String> entityIds = getEntityIds( entitySetId, syncIds );
        Iterable<ResultSetFuture> entityFutures = Iterables
                .transform( entityIds, entityId -> asyncLoadEntity( entityId, syncIds, authorizedProperties ) );
        Iterable<ResultSet> entityRows = Iterables.transform( entityFutures, ResultSetFuture::getUninterruptibly );
        return Iterables.transform( entityRows, rs -> RowAdapters.entity( rs, authorizedPropertyTypes ) );
    }

    public Iterable<String> getEntityIds( UUID entitySetId, Set<UUID> syncIds ) {
        BoundStatement boundEntityIdsQuery = entityIdsQuery.bind()
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                .setSet( CommonColumns.SYNCIDS.cql(), syncIds );
        ResultSet entityIds = session.execute( boundEntityIdsQuery );
        return Iterables.filter( Iterables.transform( entityIds, RowAdapters::entityId ), StringUtils::isNotBlank );
    }

    private ResultSetFuture asyncLoadEntity( String entityId, Set<UUID> syncIds, Set<UUID> authorizedProperties ) {
        return session.executeAsync( entitySetQuery.bind()
                .setString( CommonColumns.ENTITYID.cql(), entityId )
                .setSet( CommonColumns.SYNCIDS.cql(), syncIds )
                .setSet( CommonColumns.PROPERTY_TYPE_ID.cql(), authorizedProperties ) );
    }

    public void createEntityData( CreateEntityRequest req, Set<UUID> authorizedProperties ) {
        Set<Entity> entities = req.getEntities();
        UUID syncId = req.getSyncId();
        UUID entitySetId = req.getEntitySetId();

        List<ResultSetFuture> results = entities.stream().map( entity -> {
            BatchStatement batch = new BatchStatement();

            batch.add( writeIdLookupQuery.bind().setUUID( CommonColumns.SYNCIDS.cql(), syncId )
                    .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                    .setString( CommonColumns.ENTITYID.cql(), entity.getId() ) );

            SetMultimap<UUID, Object> propertyValues = entity.getPropertyValues();

            propertyValues.entries().stream().filter( entry -> authorizedProperties.contains( entry.getKey() ) )
                    .forEach( entry -> {
                        try {
                            batch.add( writeDataQuery.bind()
                                    .setBytes( VALUE_COLUMN.cql(), serialize( entry.getValue() ) )
                                    .setString( CommonColumns.ENTITYID.cql(), entity.getId() )
                                    .setUUID( CommonColumns.SYNCIDS.cql(), syncId )
                                    .setUUID( CommonColumns.PROPERTY_TYPE_ID.cql(), entry.getKey() ) );
                        } catch ( IOException e ) {
                            // TODO Auto-generated catch block
                            logger.debug( "Serialization error for " + entry );
                        }
                    } );

            return session.executeAsync( batch );
        } ).collect( Collectors.toList() );

        try {
            Futures.allAsList( results ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Error writing data.", e );
        }
    }

    private static PreparedStatement prepareEntitySetQuery(
            Session session,
            CassandraTableBuilder ctb ) {
        return session.prepare( entitySetQuery( ctb ) );
    }

    private static PreparedStatement prepareWriteIdLookupQuery(
            Session session,
            CassandraTableBuilder ctb ) {
        return session.prepare( writeIdLookupQuery( ctb ) );
    }

    private static PreparedStatement prepareWriteDataQuery(
            Session session,
            CassandraTableBuilder ctb ) {
        return session.prepare( writeDataQuery( ctb ) );
    }

    private static Insert writeIdLookupQuery( CassandraTableBuilder ctb ) {
        return ctb.buildStoreQuery().ifNotExists();
    }

    private static Update.Where writeDataQuery( CassandraTableBuilder ctb ) {
        // WARNING: Keep VALUE_COLUMN bindMarker at the first spot - currently createEntityData depends on that
        return QueryBuilder.update( ctb.getKeyspace().get(), ctb.getName() )
                .with( QueryBuilder.set( VALUE_COLUMN.cql(), QueryBuilder.bindMarker() ) )
                .where( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.SYNCIDS.cql(), CommonColumns.SYNCIDS.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE_ID.cql(), QueryBuilder.bindMarker() ) );
    }

    private static Select.Where entitySetQuery( CassandraTableBuilder ctb ) {
        return ctb.buildLoadAllQuery().where( QueryBuilder
                .eq( CommonColumns.ENTITYID.cql(), CommonColumns.ENTITYID.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.SYNCIDS.cql(), QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.PROPERTY_TYPE_ID.cql(), QueryBuilder.bindMarker() ) );
    }

    private static PreparedStatement prepareEntityIdsQuery( String keyspace, Session session ) {
        return session.prepare( QueryBuilder
                .select( CommonColumns.ENTITYID.cql() )
                .from( keyspace, Tables.ENTITY_ID_LOOKUP.getName() )
                .where( QueryBuilder.eq( CommonColumns.ENTITY_SET_ID.cql(), QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.SYNCIDS.cql(), QueryBuilder.bindMarker() ) ) );
    }

    private static void prepareTables(
            Session session,
            CassandraTableBuilder... ctbs ) {
        for ( CassandraTableBuilder ctb : ctbs ) {
            logger.debug( "Ensuring table " + ctb.getKeyspace() + "." + ctb.getName() + " exists." );
            session.execute( ctb.buildQuery() );
            logger.debug( "Table " + ctb.getKeyspace() + "." + ctb.getName() + " exists." );
        }
    }

    private static CassandraTableBuilder defineIdLookupTables( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_ID_LOOKUP.name() )
                .ifNotExists()
                .partitionKey( CommonColumns.SYNCIDS, CommonColumns.ENTITY_SET_ID )
                .clusteringColumns( CommonColumns.ENTITYID );
    }

    private static CassandraTableBuilder defineDataTables( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.DATA.name() )
                .ifNotExists()
                .partitionKey( CommonColumns.ENTITYID )
                .clusteringColumns( CommonColumns.SYNCIDS, CommonColumns.PROPERTY_TYPE_ID, VALUE_COLUMN );
    }

    /**
     * Serialization Utils
     * @throws IOException 
     */
    private static ByteBuffer serialize( Object obj ) throws IOException {
        try ( ByteArrayOutputStream b = new ByteArrayOutputStream() ) {
            try ( ObjectOutputStream o = new ObjectOutputStream( b ) ) {
                o.writeObject( obj );
            }
            return ByteBuffer.wrap( b.toByteArray() );
        }
    }
}
