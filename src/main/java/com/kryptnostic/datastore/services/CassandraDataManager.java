package com.kryptnostic.datastore.services;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
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
    private final PreparedStatement writeQueries;
    private final PreparedStatement entitySetQuery;
    private final PreparedStatement entityIdsQuery;

    public CassandraDataManager( String keyspace, Session session ) {
        this.keyspace = keyspace;
        this.session = session;
        CassandraTableBuilder dataTableDefinitions = defineDataTables( keyspace );
        prepareDataTables( session, dataTableDefinitions );
        this.entitySetQuery = prepareEntitySetQuery( session, dataTableDefinitions );
        this.entityIdsQuery = prepareEntityIdsQuery( keyspace, session );
        this.writeQueries = prepareWriteQueries( session, dataTableDefinitions );
    }

    public Iterable<SetMultimap<FullQualifiedName, Object>> getEntitySetData(
            String entitySetName,
            Map<UUID, CassandraPropertyReader> authorizedPropertyTypes ) {
        Set<UUID> authorizedProperties = authorizedPropertyTypes.keySet();
        Iterable<String> entityIds = getEntityIds( entitySetName, authorizedProperties );
        Iterable<ResultSetFuture> entityFutures = Iterables
                .transform( entityIds, entityId -> asyncLoadEntity( entityId, authorizedProperties ) );
        Iterable<ResultSet> entityRows = Iterables.transform( entityFutures, ResultSetFuture::getUninterruptibly );
        return Iterables.transform( entityRows, rs -> RowAdapters.entity( rs, authorizedPropertyTypes ) );
    }

    public Iterable<String> getEntityIds( String entitySetName, Set<UUID> authorizedProperties ) {
        BoundStatement boundEntityIdsQuery = entityIdsQuery.bind()
                .setString( CommonColumns.ENTITY_SETS.cql(), entitySetName )
                .setSet( CommonColumns.PROPERTY_TYPE.cql(), authorizedProperties );
        ResultSet entityIds = session.execute( boundEntityIdsQuery );
        return Iterables.filter( Iterables.transform( entityIds, RowAdapters::entityId ), StringUtils::isNotBlank );
    }

    private ResultSetFuture asyncLoadEntity( String entityId, Set<UUID> authorizedProperties ) {
        return session.executeAsync( entitySetQuery.bind()
                .setString( CommonColumns.ENTITYID.cql(), entityId )
                .setSet( CommonColumns.PROPERTY_TYPE.cql(), authorizedProperties ) );
    }

    private static PreparedStatement prepareEntitySetQuery(
            Session session,
            CassandraTableBuilder ctb ) {
        return session.prepare( entityQuery( ctb ) );
    }

    private static PreparedStatement prepareWriteQueries(
            Session session,
            CassandraTableBuilder ctb ) {
        return session.prepare( writeQuery( ctb ) );
    }

    private static Update.Where writeQuery( CassandraTableBuilder ctb ) {
        return QueryBuilder.update( ctb.getKeyspace().get(), ctb.getName() )
                .with( QueryBuilder.add( CommonColumns.SYNCIDS.cql(), CommonColumns.SYNCIDS.bindMarker() ) )
                .and( QueryBuilder.add( CommonColumns.ENTITY_SET.cql(), CommonColumns.ENTITY_SET.bindMarker() ) )
                .where( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( VALUE_COLUMN.cql(), QueryBuilder.bindMarker() ) );
    }

    private static Select.Where entityQuery( CassandraTableBuilder ctb ) {
        return ctb.buildLoadAllQuery().where( QueryBuilder
                .eq( CommonColumns.ENTITYID.cql(), CommonColumns.ENTITYID.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) );
    }

    private static PreparedStatement prepareEntityIdsQuery( String keyspace, Session session ) {
        return session.prepare( QueryBuilder
                .select( CommonColumns.ENTITYID.cql() ).distinct()
                .from( keyspace, Tables.DATA.getName() )
                .where( QueryBuilder.contains( CommonColumns.ENTITY_SETS.cql(),
                        CommonColumns.ENTITY_SETS.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.PROPERTY_TYPE.cql(), QueryBuilder.bindMarker() ) ) );
    }

    private static void prepareDataTables(
            Session session,
            CassandraTableBuilder ctb ) {
        logger.debug( "Ensuring data table exists." );
        session.execute( ctb.buildQuery() );
        logger.debug( "Data table exists." );
    }

    private static CassandraTableBuilder defineDataTables( String keyspace ) {
        return new CassandraTableBuilder( keyspace, Tables.DATA.name() )
                .ifNotExists()
                .partitionKey( CommonColumns.ENTITYID )
                .clusteringColumns( CommonColumns.PROPERTY_TYPE_ID )
                .columns( VALUE_COLUMN, CommonColumns.SYNCIDS, CommonColumns.ENTITY_SETS );
    }

}
