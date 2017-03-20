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

package com.dataloom.datastore.services;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.EntityKey;
import com.dataloom.data.events.EntityDataCreatedEvent;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.streams.StreamUtil;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CassandraSerDesFactory;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;

public class CassandraDataManager {

    @Inject
    private EventBus                     eventBus;

    private static final Logger          logger = LoggerFactory
            .getLogger( CassandraDataManager.class );

    private final Session                session;
    private final ObjectMapper           mapper;
    private final HazelcastLinkingGraphs linkingGraph;

    private final PreparedStatement      writeDataQuery;

    private final PreparedStatement      entitySetQueryUpToSyncId;
    private final PreparedStatement      entitySetQuery;
    private final PreparedStatement      entityIdsQuery;

    private final PreparedStatement      entityIdsLookupByEntitySetQuery;
    private final PreparedStatement      deleteEntityQuery;

    private final PreparedStatement      linkedEntitiesQuery;

    private final PreparedStatement      readNumRPCRowsQuery;

    public CassandraDataManager( Session session, ObjectMapper mapper, HazelcastLinkingGraphs linkingGraph ) {
        this.session = session;
        this.mapper = mapper;
        this.linkingGraph = linkingGraph;

        CassandraTableBuilder dataTableDefinitions = Table.DATA.getBuilder();

        this.entitySetQueryUpToSyncId = prepareEntitySetQueryUpToSyncId( session, dataTableDefinitions );
        this.entitySetQuery = prepareEntitySetQuery( session, dataTableDefinitions );
        this.entityIdsQuery = prepareEntityIdsQuery( session );
        this.writeDataQuery = prepareWriteQuery( session, dataTableDefinitions );

        this.entityIdsLookupByEntitySetQuery = prepareEntityIdsLookupByEntitySetQuery( session );
        this.deleteEntityQuery = prepareDeleteEntityQuery( session );

        this.linkedEntitiesQuery = prepareLinkedEntitiesQuery( session );
        this.readNumRPCRowsQuery = prepareReadNumRPCRowsQuery( session );
    }

    public Iterable<SetMultimap<FullQualifiedName, Object>> getEntitySetData(
            UUID entitySetId,
            UUID syncId,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        Iterable<ResultSet> entityRows = getRows( entitySetId, syncId, authorizedPropertyTypes.keySet() );
        return Iterables.transform( entityRows,
                rs -> rowToEntity( rs, authorizedPropertyTypes ) )::iterator;
    }

    public Iterable<SetMultimap<UUID, Object>> getEntitySetDataIndexedById(
            UUID entitySetId,
            UUID syncId,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        Iterable<ResultSet> entityRows = getRows( entitySetId, syncId, authorizedPropertyTypes.keySet() );
        return Iterables.transform( entityRows,
                rs -> rowToEntityIndexedById( rs, authorizedPropertyTypes ) )::iterator;
    }

    public Iterable<SetMultimap<FullQualifiedName, Object>> getLinkedEntitySetData(
            UUID linkedEntitySetId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets ) {
        Iterable<Pair<UUID, Set<EntityKey>>> linkedEntityKeys = getLinkedEntityKeys( linkedEntitySetId );
        return Iterables.transform( linkedEntityKeys,
                linkedKey -> getAndMergeLinkedEntities( linkedEntitySetId,
                        linkedKey,
                        authorizedPropertyTypesForEntitySets ) )::iterator;
    }

    public SetMultimap<FullQualifiedName, Object> rowToEntity(
            ResultSet rs,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        return RowAdapters.entity( rs, authorizedPropertyTypes, mapper );
    }

    public SetMultimap<UUID, Object> rowToEntityIndexedById(
            ResultSet rs,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        return RowAdapters.entityIndexedById( rs, authorizedPropertyTypes, mapper );
    }

    private Iterable<ResultSet> getRows(
            UUID entitySetId,
            UUID syncId,
            Set<UUID> authorizedProperties ) {
        Iterable<String> entityIds = getEntityIds( entitySetId );
        Iterable<ResultSetFuture> entityFutures; 
        //If syncId is not specified, retrieve latest snapshot of entity
        if( syncId != null ){
            entityFutures = Iterables.transform( entityIds,
                    entityId -> asyncLoadEntity( entitySetId, entityId, syncId, authorizedProperties ) );
        } else {
            entityFutures = Iterables.transform( entityIds,
                    entityId -> asyncLoadEntity( entitySetId, entityId, authorizedProperties ) );
        }
        return Iterables.transform( entityFutures, ResultSetFuture::getUninterruptibly );
    }

    // TODO Unexposed (yet) method. Would you batch this with the previous one? If yes, their return type needs to match
    public SetMultimap<FullQualifiedName, Object> getEntity(
            UUID entitySetId,
            String entityId,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        Set<UUID> authorizedProperties = authorizedPropertyTypes.keySet();
        ResultSet rs = asyncLoadEntity( entitySetId, entityId, authorizedProperties ).getUninterruptibly();
        return RowAdapters.entity( rs, authorizedPropertyTypes, mapper );
    }

    public Iterable<String> getEntityIds( UUID entitySetId ) {
        BoundStatement boundEntityIdsQuery = entityIdsQuery.bind()
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId );
        ResultSet entityIds = session.execute( boundEntityIdsQuery );
        return Iterables.filter( Iterables.transform( entityIds, RowAdapters::entityId ), StringUtils::isNotBlank );
    }

    public ResultSetFuture asyncLoadEntity( UUID entitySetId, String entityId, Set<UUID> authorizedProperties ) {
        return session.executeAsync( entitySetQuery.bind()
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                .setString( CommonColumns.ENTITYID.cql(), entityId )
                .setSet( CommonColumns.PROPERTY_TYPE_ID.cql(), authorizedProperties ) );
    }

    public ResultSetFuture asyncLoadEntity( UUID entitySetId, String entityId, UUID syncId, Set<UUID> authorizedProperties ) {
        return session.executeAsync( entitySetQueryUpToSyncId.bind()
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                .setString( CommonColumns.ENTITYID.cql(), entityId )
                .setSet( CommonColumns.PROPERTY_TYPE_ID.cql(), authorizedProperties )
                .setUUID( CommonColumns.SYNCID.cql(), syncId ) );
    }

    public void createEntityData(
            UUID entitySetId,
            UUID syncId,
            Map<String, SetMultimap<UUID, Object>> entities,
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType ) {
        Set<UUID> authorizedProperties = authorizedPropertiesWithDataType.keySet();

        List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();

        entities.entrySet().stream().forEach( entity -> {

            SetMultimap<UUID, Object> propertyValues = entity.getValue();

            // does not write the row if some property values that user is trying to write to are not authorized.
            if ( !authorizedProperties.containsAll( propertyValues.keySet() ) ) {
                logger.info( "Entity {} not written because not all property values are authorized.", entity.getKey() );
                return;
            }

            SetMultimap<UUID, Object> normalizedPropertyValues = null;
            try {
                normalizedPropertyValues = CassandraSerDesFactory.validateFormatAndNormalize( propertyValues,
                        authorizedPropertiesWithDataType );
            } catch ( Exception e ) {
                logger.info( "Entity {} not written because some property values are of invalid format.",
                        entity.getKey() );
                return;
            }
            // Stream<Entry<UUID, Object>> authorizedPropertyValues = propertyValues.entries().stream().filter( entry ->
            // authorizedProperties.contains( entry.getKey() ) );
            normalizedPropertyValues.entries().stream()
                    .forEach( entry -> {
                        results.add( session.executeAsync(
                                writeDataQuery.bind()
                                        .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                                        .setString( CommonColumns.ENTITYID.cql(), entity.getKey() )
                                        .setUUID( CommonColumns.SYNCID.cql(), syncId )
                                        .setUUID( CommonColumns.PROPERTY_TYPE_ID.cql(), entry.getKey() )
                                        .setBytes( CommonColumns.PROPERTY_VALUE.cql(),
                                                CassandraSerDesFactory.serializeValue(
                                                        mapper,
                                                        entry.getValue(),
                                                        authorizedPropertiesWithDataType
                                                                .get( entry.getKey() ),
                                                        entity.getKey() ) ) ) );
                    } );

            Map<UUID, Object> normalizedPropertyValuesAsMap = normalizedPropertyValues.asMap().entrySet().stream()
                    .collect( Collectors.toMap( e -> e.getKey(), e -> new HashSet<>( e.getValue() ) ) );

            eventBus.post( new EntityDataCreatedEvent( entitySetId, entity.getKey(), normalizedPropertyValuesAsMap ) );
        } );

        results.forEach( ResultSetFuture::getUninterruptibly );
    }

    public void createOrderedRPCData( UUID requestId, double weight, byte[] value ) {
        session.executeAsync( writeDataQuery.bind().setUUID( CommonColumns.RPC_REQUEST_ID.cql(), requestId )
                .setDouble( CommonColumns.RPC_WEIGHT.cql(), weight )
                .setBytes( CommonColumns.RPC_VALUE.cql(), ByteBuffer.wrap( value ) ) );
    }

    public Stream<byte[]> readNumRPCRows( UUID requestId, int numResults ) {
        logger.info( "Reading {} rows of RPC data for request id {}", numResults, requestId );
        BoundStatement bs = readNumRPCRowsQuery.bind().setUUID( CommonColumns.RPC_REQUEST_ID.cql(), requestId )
                .setInt( "numResults", numResults );
        ResultSet rs = session.execute( bs );
        return StreamUtil.stream( rs )
                .map( r -> r.getBytes( CommonColumns.RPC_VALUE.cql() ).array() );
    }

    /**
     * Delete data of an entity set across ALL sync Ids.
     */
    public void deleteEntitySetData( UUID entitySetId ) {
        logger.info( "Deleting data of entity set: {}", entitySetId );
        BoundStatement bs = entityIdsLookupByEntitySetQuery.bind().setUUID( CommonColumns.ENTITY_SET_ID.cql(),
                entitySetId );
        ResultSet rs = session.execute( bs );

        List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();
        final int bufferSize = 1000;
        int counter = 0;

        for ( Row entityIdRow : rs ) {
            if ( counter > bufferSize ) {
                results.forEach( ResultSetFuture::getUninterruptibly );
                counter = 0;
                results = new ArrayList<ResultSetFuture>();
            }
            String entityId = RowAdapters.entityId( entityIdRow );

            results.add( asyncDeleteEntity( entitySetId, entityId ) );

            counter++;
        }

        results.forEach( ResultSetFuture::getUninterruptibly );
        logger.info( "Finish deletion of entity set data: {}", entitySetId );
    }

    public ResultSetFuture asyncDeleteEntity( UUID entitySetId, String entityId ) {
        return session.executeAsync( deleteEntityQuery.bind()
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                .setString( CommonColumns.ENTITYID.cql(), entityId ) );
    }

    private static PreparedStatement prepareEntitySetQuery(
            Session session,
            CassandraTableBuilder ctb ) {
        return session.prepare( entitySetQuery( ctb ) );
    }

    private static PreparedStatement prepareEntitySetQueryUpToSyncId(
            Session session,
            CassandraTableBuilder ctb ) {
        return session.prepare( entitySetQueryUpToSyncId( ctb ) );
    }

    private static PreparedStatement prepareWriteQuery(
            Session session,
            CassandraTableBuilder ctb ) {
        return session.prepare( writeQuery( ctb ) );
    }

    private static Insert writeQuery( CassandraTableBuilder ctb ) {
        return ctb.buildStoreQuery();
    }

    private static Select.Where entitySetQuery( CassandraTableBuilder ctb ) {
        return ctb.buildLoadAllQuery().where( QueryBuilder
                .eq( CommonColumns.ENTITY_SET_ID.cql(), CommonColumns.ENTITY_SET_ID.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), CommonColumns.ENTITYID.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.PROPERTY_TYPE_ID.cql(),
                        CommonColumns.PROPERTY_TYPE_ID.bindMarker() ) );
    }

    private static Select.Where entitySetQueryUpToSyncId( CassandraTableBuilder ctb ) {
        return ctb.buildLoadAllQuery().where( QueryBuilder
                .eq( CommonColumns.ENTITY_SET_ID.cql(), CommonColumns.ENTITY_SET_ID.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.ENTITYID.cql(), CommonColumns.ENTITYID.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.PROPERTY_TYPE_ID.cql(),
                        CommonColumns.PROPERTY_TYPE_ID.bindMarker() ) )
                .and( QueryBuilder.lte( CommonColumns.SYNCID.cql(), CommonColumns.SYNCID.bindMarker() ) );
    }

    private static PreparedStatement prepareEntityIdsQuery( Session session ) {
        return session.prepare( QueryBuilder
                .select()
                .column( CommonColumns.ENTITY_SET_ID.cql() ).column( CommonColumns.ENTITYID.cql() )
                .distinct()
                .from( Table.DATA.getKeyspace(), Table.DATA.getName() )
                .where( QueryBuilder.eq( CommonColumns.ENTITY_SET_ID.cql(), QueryBuilder.bindMarker() ) ) );
    }

    private static PreparedStatement prepareLinkedEntitiesQuery( Session session ) {
        return session.prepare( QueryBuilder
                .select().all()
                .from( Table.LINKING_VERTICES.getKeyspace(), Table.LINKING_VERTICES.getName() ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.GRAPH_ID.cql(), QueryBuilder.bindMarker() ) ) );
    }

    private static PreparedStatement prepareEntityIdsLookupByEntitySetQuery(
            Session session ) {
        return session.prepare( QueryBuilder
                .select()
                .from( Table.DATA.getKeyspace(), Table.DATA.getName() )
                .where( QueryBuilder.eq( CommonColumns.ENTITY_SET_ID.cql(), QueryBuilder.bindMarker() ) ) );
    }

    private static PreparedStatement prepareReadNumRPCRowsQuery( Session session ) {
        return session.prepare(
                QueryBuilder.select().from( Table.RPC_DATA_ORDERED.getKeyspace(), Table.RPC_DATA_ORDERED.getName() )
                        .where( QueryBuilder.eq( CommonColumns.RPC_REQUEST_ID.cql(),
                                CommonColumns.RPC_REQUEST_ID.bindMarker() ) )
                        .limit( QueryBuilder.bindMarker( "numResults" ) ) );
    }

    private static PreparedStatement prepareDeleteEntityQuery(
            Session session ) {
        return session.prepare( Table.DATA.getBuilder().buildDeleteByPartitionKeyQuery() );
    }

    /**
     * Auxiliary methods for linking entity sets
     */

    private Iterable<Pair<UUID, Set<EntityKey>>> getLinkedEntityKeys(
            UUID linkedEntitySetId ) {
        UUID graphId = linkingGraph.getGraphIdFromEntitySetId( linkedEntitySetId );
        ResultSet rs = session
                .execute( linkedEntitiesQuery.bind().setUUID( CommonColumns.GRAPH_ID.cql(), graphId ) );
        return Iterables.transform( rs, RowAdapters::linkedEntity );
    }

    private SetMultimap<FullQualifiedName, Object> getAndMergeLinkedEntities(
            UUID linkedEntitySetId,
            Pair<UUID, Set<EntityKey>> linkedKey,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets ) {
        SetMultimap<FullQualifiedName, Object> result = HashMultimap.create();
        SetMultimap<UUID, Object> indexResult = HashMultimap.create();

        linkedKey.getValue().stream()
                .map( key -> Pair.of( key.getEntitySetId(),
                        asyncLoadEntity( key.getEntitySetId(), key.getEntityId(),
                                authorizedPropertyTypesForEntitySets.get( key.getEntitySetId() ).keySet() ) ) )
                .map( rsfPair -> Pair.of( rsfPair.getKey(), rsfPair.getValue().getUninterruptibly() ) )
                .map( rsPair -> RowAdapters.entityIdFQNPair( rsPair.getValue(),
                        authorizedPropertyTypesForEntitySets.get( rsPair.getKey() ),
                        mapper ) )
                .forEach( pair -> {
                    result.putAll( pair.getValue() );
                    indexResult.putAll( pair.getKey() );
                } );

        // Using HashSet here is necessary for serialization, to avoid kryo not knowing how to serialize guava
        // WrappedCollection
        Map<UUID, Object> indexResultAsMap = indexResult.asMap().entrySet().stream()
                .collect( Collectors.toMap( e -> e.getKey(), e -> new HashSet<>( e.getValue() ) ) );

        eventBus.post(
                new EntityDataCreatedEvent( linkedEntitySetId, linkedKey.getKey().toString(), indexResultAsMap ) );
        return result;
    }
}
