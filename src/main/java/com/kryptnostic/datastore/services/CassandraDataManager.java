package com.kryptnostic.datastore.services;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
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
import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
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
        this.writeIdLookupQuery = prepareWriteQuery( session, idLookupTableDefinitions );
        this.writeDataQuery = prepareWriteQuery( session, dataTableDefinitions );
    }

    public Iterable<Entity> getEntitySetData(
            UUID entitySetId,
            Set<UUID> syncIds,
            Map<UUID, CassandraPropertyReader> authorizedPropertyTypes ) {
        Set<UUID> authorizedProperties = authorizedPropertyTypes.keySet();
        Iterable<String> entityIds = getEntityIds( entitySetId, syncIds );
        Map<String, ResultSetFuture> entityFutures = Maps.toMap( entityIds,
                entityId -> asyncLoadEntity( entityId, syncIds, authorizedProperties ) );
        Map<String, ResultSet> entityRows = Maps.transformValues( entityFutures, ResultSetFuture::getUninterruptibly );
        return Iterables.transform( entityRows.entrySet(),
                entry -> new Entity(
                        entry.getKey(),
                        RowAdapters.entity( entry.getValue(), authorizedPropertyTypes ) ) );
    }

    public Iterable<String> getEntityIds( UUID entitySetId, Set<UUID> syncIds ) {
        BoundStatement boundEntityIdsQuery = entityIdsQuery.bind()
                .setSet( CommonColumns.SYNCID.cql(), syncIds )
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId );
        ResultSet entityIds = session.execute( boundEntityIdsQuery );
        return Iterables.filter( Iterables.transform( entityIds, RowAdapters::entityId ), StringUtils::isNotBlank );
    }

    private ResultSetFuture asyncLoadEntity( String entityId, Set<UUID> syncIds, Set<UUID> authorizedProperties ) {
        return session.executeAsync( entitySetQuery.bind()
                .setString( CommonColumns.ENTITYID.cql(), entityId )
                .setSet( CommonColumns.SYNCID.cql(), syncIds )
                .setSet( CommonColumns.PROPERTY_TYPE_ID.cql(), authorizedProperties ) );
    }

    public void createEntityData( CreateEntityRequest req, Set<UUID> authorizedProperties ) {
        Set<Entity> entities = req.getEntities();
        UUID syncId = req.getSyncId();
        UUID entitySetId = req.getEntitySetId();

        List<ResultSetFuture> results = entities.stream().map( entity -> {
            BatchStatement batch = new BatchStatement();

            batch.add( writeIdLookupQuery.bind().setUUID( CommonColumns.SYNCID.cql(), syncId )
                    .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                    .setString( CommonColumns.ENTITYID.cql(), entity.getId() ) );

            SetMultimap<UUID, Object> propertyValues = entity.getPropertyValues();

            propertyValues.entries().stream().filter( entry -> authorizedProperties.contains( entry.getKey() ) )
                    .forEach( entry -> {
                        try {
                            batch.add( writeDataQuery.bind()
                                    .setBytes( VALUE_COLUMN.cql(), serialize( entry.getValue() ) )
                                    .setString( CommonColumns.ENTITYID.cql(), entity.getId() )
                                    .setUUID( CommonColumns.SYNCID.cql(), syncId )
                                    .setUUID( CommonColumns.PROPERTY_TYPE_ID.cql(), entry.getKey() ) );
                        } catch ( IOException e ) {
                            logger.error( "Serialization error when writing entry " + entry );
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

    private static PreparedStatement prepareWriteQuery(
            Session session,
            CassandraTableBuilder ctb ) {
        return session.prepare( writeQuery( ctb ) );
    }

    private static Insert writeQuery( CassandraTableBuilder ctb ) {
        // return ctb.buildStoreQuery().ifNotExists();
        return ctb.buildStoreQuery();
    }

    private static Select.Where entitySetQuery( CassandraTableBuilder ctb ) {
        return ctb.buildLoadAllQuery().where( QueryBuilder
                .eq( CommonColumns.ENTITYID.cql(), CommonColumns.ENTITYID.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.SYNCID.cql(), CommonColumns.SYNCID.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.PROPERTY_TYPE_ID.cql(), CommonColumns.PROPERTY_TYPE_ID.bindMarker() ) );
    }

    private static PreparedStatement prepareEntityIdsQuery( String keyspace, Session session ) {
        return session.prepare( QueryBuilder
                .select( CommonColumns.ENTITYID.cql() )
                .from( keyspace, Tables.ENTITY_ID_LOOKUP.getName() )
                .where( QueryBuilder.eq( CommonColumns.ENTITY_SET_ID.cql(), QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.SYNCID.cql(), CommonColumns.SYNCID.bindMarker() ) ) );
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
        return new CassandraTableBuilder( keyspace, Tables.ENTITY_ID_LOOKUP.getName() )
                .ifNotExists()
                .partitionKey( CommonColumns.SYNCID, CommonColumns.ENTITY_SET_ID )
                .clusteringColumns( CommonColumns.ENTITYID );
    }

    private static CassandraTableBuilder defineDataTables( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.DATA.getName() )
                .ifNotExists()
                .partitionKey( CommonColumns.ENTITYID )
                .clusteringColumns( CommonColumns.SYNCID, CommonColumns.PROPERTY_TYPE_ID, VALUE_COLUMN );
    }

    /**
     * Serialization Utils
     * 
     * @throws IOException
     */
    public static ByteBuffer serialize( Object obj ) throws IOException {
        try ( ByteArrayOutputStream b = new ByteArrayOutputStream() ) {
            try ( ObjectOutputStream o = new ObjectOutputStream( b ) ) {
                o.writeObject( obj );
            }
            return ByteBuffer.wrap( b.toByteArray() );
        }
    }

    public static Object deserialize( ByteBuffer buf ) throws IOException, ClassNotFoundException {
        try ( ByteArrayInputStream b = new ByteArrayInputStream( Bytes.getArray( buf ) ) ) {
            try ( ObjectInputStream i = new ObjectInputStream( b ) ) {
                return i.readObject();
            }
        }
    }
}
