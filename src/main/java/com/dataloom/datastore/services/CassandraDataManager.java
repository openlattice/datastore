package com.dataloom.datastore.services;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.edm.internal.PropertyType;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.Futures;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.datastore.util.Util;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;

public class CassandraDataManager {
    private static final Logger     logger = LoggerFactory
            .getLogger( CassandraDataManager.class );
    private final Session           session;
    private final ObjectMapper      mapper;
    private final PreparedStatement writeIdLookupQuery;
    private final PreparedStatement writeDataQuery;
    private final PreparedStatement entitySetQuery;
    private final PreparedStatement entityIdsQuery;

    public CassandraDataManager( Session session, ObjectMapper mapper ) {
        this.session = session;
        this.mapper = mapper;
        CassandraTableBuilder idLookupTableDefinitions = Tables.ENTITY_ID_LOOKUP.getBuilder();
        CassandraTableBuilder dataTableDefinitions = Tables.DATA.getBuilder();

        this.entitySetQuery = prepareEntitySetQuery( session, dataTableDefinitions );
        this.entityIdsQuery = prepareEntityIdsQuery( session );
        this.writeIdLookupQuery = prepareWriteQuery( session, idLookupTableDefinitions );
        this.writeDataQuery = prepareWriteQuery( session, dataTableDefinitions );
    }

    public Iterable<SetMultimap<FullQualifiedName, Object>> getEntitySetData(
            UUID entitySetId,
            Set<UUID> syncIds,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        Set<UUID> authorizedProperties = authorizedPropertyTypes.keySet();
        Iterable<String> entityIds = getEntityIds( entitySetId, syncIds );
        Iterable<ResultSetFuture> entityFutures = Iterables.transform( entityIds,
                entityId -> asyncLoadEntity( entityId, syncIds, authorizedProperties ) );
        Iterable<ResultSet> entityRows = Iterables.transform( entityFutures, ResultSetFuture::getUninterruptibly );
        return Iterables.transform( entityRows, rs -> RowAdapters.entity( rs, authorizedPropertyTypes, mapper ) )::iterator;
    }

    // TODO Unexposed (yet) method. Would you batch this with the previous one? If yes, their return type needs to match
    public SetMultimap<FullQualifiedName, Object> getEntity(
            UUID entitySetId,
            String entityId,
            Set<UUID> syncIds,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        Set<UUID> authorizedProperties = authorizedPropertyTypes.keySet();
        ResultSet rs = asyncLoadEntity( entityId, syncIds, authorizedProperties ).getUninterruptibly();
        return RowAdapters.entity( rs, authorizedPropertyTypes, mapper );
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

    public void createEntityData(
            UUID entitySetId,
            UUID syncId,
            Map<String, SetMultimap<UUID, Object>> entities,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        Set<UUID> authorizedProperties = authorizedPropertiesWithDataType.keySet();

        List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();

        entities.entrySet().stream().forEach( entity -> {

            boolean idWritten = Util.wasLightweightTransactionApplied(
                    session.execute( writeIdLookupQuery.bind().setUUID( CommonColumns.SYNCID.cql(), syncId )
                            .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                            .setString( CommonColumns.ENTITYID.cql(), entity.getKey() ) ) );

            if ( !idWritten ) {
                logger.error( "Failed to write entity " + entity.getKey() + " into id table." );
                throw new IllegalStateException( "Failed to write entity " + entity.getKey() );
            }

            SetMultimap<UUID, Object> propertyValues = entity.getValue();

            propertyValues.entries().stream().filter( entry -> authorizedProperties.contains( entry.getKey() ) )
                    .forEach( entry -> {
                        results.add( session.executeAsync(
                                writeDataQuery.bind()
                                        .setString( CommonColumns.ENTITYID.cql(), entity.getKey() )
                                        .setUUID( CommonColumns.SYNCID.cql(), syncId )
                                        .setUUID( CommonColumns.PROPERTY_TYPE_ID.cql(), entry.getKey() )
                                        .setBytes( CommonColumns.PROPERTY_VALUE.cql(),
                                                RowAdapters.serializeValue( mapper,
                                                        entry.getValue(),
                                                        authorizedPropertiesWithDataType
                                                                .get( entry.getKey() ),
                                                        entity.getKey() ) ) ) );
                    } );
        } );

        results.forEach( ResultSetFuture::getUninterruptibly );
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
        return ctb.buildStoreQuery().ifNotExists();
    }

    private static Select.Where entitySetQuery( CassandraTableBuilder ctb ) {
        return ctb.buildLoadAllQuery().where( QueryBuilder
                .eq( CommonColumns.ENTITYID.cql(), CommonColumns.ENTITYID.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.SYNCID.cql(), CommonColumns.SYNCID.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.PROPERTY_TYPE_ID.cql(),
                        CommonColumns.PROPERTY_TYPE_ID.bindMarker() ) );
    }

    private static PreparedStatement prepareEntityIdsQuery( Session session ) {
        return session.prepare( QueryBuilder
                .select( CommonColumns.ENTITYID.cql() )
                .from( Tables.ENTITY_ID_LOOKUP.getKeyspace(), Tables.ENTITY_ID_LOOKUP.getName() )
                .where( QueryBuilder.eq( CommonColumns.ENTITY_SET_ID.cql(), QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.SYNCID.cql(), CommonColumns.SYNCID.bindMarker() ) ) );
    }
}
