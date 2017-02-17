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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
import com.dataloom.mappers.ObjectMappers;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.kryptnostic.conductor.rpc.odata.Table;
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

    private final PreparedStatement      writeIdLookupQuery;
    private final PreparedStatement      writeDataQuery;
    private final PreparedStatement      entitySetQuery;
    private final PreparedStatement      entityIdsQuery;
    private final PreparedStatement      linkedEntitiesQuery;

    public CassandraDataManager( Session session, ObjectMapper mapper, HazelcastLinkingGraphs linkingGraph ) {
        this.session = session;
        this.mapper = mapper;
        this.linkingGraph = linkingGraph;

        CassandraTableBuilder idLookupTableDefinitions = Table.ENTITY_ID_LOOKUP.getBuilder();
        CassandraTableBuilder dataTableDefinitions = Table.DATA.getBuilder();

        this.entitySetQuery = prepareEntitySetQuery( session, dataTableDefinitions );
        this.entityIdsQuery = prepareEntityIdsQuery( session );
        this.writeIdLookupQuery = prepareWriteQuery( session, idLookupTableDefinitions );
        this.writeDataQuery = prepareWriteQuery( session, dataTableDefinitions );
        this.linkedEntitiesQuery = prepareLinkedEntitiesQuery( session );
    }

    public Iterable<SetMultimap<FullQualifiedName, Object>> getEntitySetData(
            UUID entitySetId,
            Set<UUID> syncIds,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        Iterable<ResultSet> entityRows = getRows( entitySetId, syncIds, authorizedPropertyTypes.keySet() );
        return Iterables.transform( entityRows,
                rs -> rowToEntity( rs, authorizedPropertyTypes ) )::iterator;
    }

    public Iterable<SetMultimap<UUID, Object>> getEntitySetDataIndexedById(
            UUID entitySetId,
            Set<UUID> syncIds,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        Iterable<ResultSet> entityRows = getRows( entitySetId, syncIds, authorizedPropertyTypes.keySet() );
        return Iterables.transform( entityRows,
                rs -> rowToEntityIndexedById( rs, authorizedPropertyTypes ) )::iterator;
    }

    public Iterable<SetMultimap<FullQualifiedName, Object>> getLinkedEntitySetData(
            UUID linkedEntitySetId,
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesForEntitySets ) {
        Iterable<Pair<UUID,Set<EntityKey>>> linkedEntityKeys = getLinkedEntityKeys( linkedEntitySetId );
        return Iterables.transform( linkedEntityKeys,
                linkedKey -> getAndMergeLinkedEntities( linkedEntitySetId, linkedKey, authorizedPropertyTypesForEntitySets ) )::iterator;
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
            Set<UUID> syncIds,
            Set<UUID> authorizedProperties ) {
        Iterable<String> entityIds = getEntityIds( entitySetId, syncIds );
        Iterable<ResultSetFuture> entityFutures = Iterables.transform( entityIds,
                entityId -> asyncLoadEntity( entityId, authorizedProperties ) );
        return Iterables.transform( entityFutures, ResultSetFuture::getUninterruptibly );
    }

    // TODO Unexposed (yet) method. Would you batch this with the previous one? If yes, their return type needs to match
    public SetMultimap<FullQualifiedName, Object> getEntity(
            UUID entitySetId,
            String entityId,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        Set<UUID> authorizedProperties = authorizedPropertyTypes.keySet();
        ResultSet rs = asyncLoadEntity( entityId, authorizedProperties ).getUninterruptibly();
        return RowAdapters.entity( rs, authorizedPropertyTypes, mapper );
    }

    public Iterable<String> getEntityIds( UUID entitySetId, Set<UUID> syncIds ) {
        BoundStatement boundEntityIdsQuery = entityIdsQuery.bind()
                .setSet( CommonColumns.SYNCID.cql(), syncIds )
                .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId );
        ResultSet entityIds = session.execute( boundEntityIdsQuery );
        return Iterables.filter( Iterables.transform( entityIds, RowAdapters::entityId ), StringUtils::isNotBlank );
    }

    public ResultSetFuture asyncLoadEntity( String entityId, Set<UUID> authorizedProperties ) {
        return session.executeAsync( entitySetQuery.bind()
                .setString( CommonColumns.ENTITYID.cql(), entityId )
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

            results.add(
                    session.executeAsync( writeIdLookupQuery.bind().setUUID( CommonColumns.SYNCID.cql(), syncId )
                            .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                            .setString( CommonColumns.ENTITYID.cql(), entity.getKey() ) ) );

            SetMultimap<UUID, Object> propertyValues = entity.getValue();

            Map<UUID, String> authorizedPropertyValues = Maps.newHashMap();
            // Stream<Entry<UUID, Object>> authorizedPropertyValues = propertyValues.entries().stream().filter( entry ->
            // authorizedProperties.contains( entry.getKey() ) );
            propertyValues.entries().stream()
                    .filter( entry -> authorizedProperties.contains( entry.getKey() ) )
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
                        // TODO: wtf move this
                        try {
                            authorizedPropertyValues.put( entry.getKey(),
                                    ObjectMappers.getJsonMapper().writeValueAsString( entry.getValue() ) );
                        } catch ( JsonProcessingException e ) {
                            e.printStackTrace();
                        }
                    } );
            eventBus.post( new EntityDataCreatedEvent( entitySetId, entity.getKey(), authorizedPropertyValues ) );
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
        return ctb.buildStoreQuery();
    }

    private static Select.Where entitySetQuery( CassandraTableBuilder ctb ) {
        return ctb.buildLoadAllQuery().where( QueryBuilder
                .eq( CommonColumns.ENTITYID.cql(), CommonColumns.ENTITYID.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.PROPERTY_TYPE_ID.cql(),
                        CommonColumns.PROPERTY_TYPE_ID.bindMarker() ) );
    }

    private static PreparedStatement prepareEntityIdsQuery( Session session ) {
        return session.prepare( QueryBuilder
                .select( CommonColumns.ENTITYID.cql() )
                .from( Table.ENTITY_ID_LOOKUP.getKeyspace(), Table.ENTITY_ID_LOOKUP.getName() )
                .where( QueryBuilder.eq( CommonColumns.ENTITY_SET_ID.cql(), QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.in( CommonColumns.SYNCID.cql(), CommonColumns.SYNCID.bindMarker() ) ) );
    }

    private static PreparedStatement prepareLinkedEntitiesQuery( Session session ) {
        return session.prepare( QueryBuilder
                .select().all()
                .from( Table.LINKING_VERTICES.getKeyspace(), Table.LINKING_VERTICES.getName() ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.GRAPH_ID.cql(), QueryBuilder.bindMarker() ) ) );
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
        Map<UUID, String> indexResult = Maps.newHashMap();

        linkedKey.getValue().stream()
                .map( key -> Pair.of( key.getEntitySetId(),
                        asyncLoadEntity( key.getEntityId(),
                                authorizedPropertyTypesForEntitySets.get( key.getEntitySetId() ).keySet() ) ) )
                .map( rsfPair -> Pair.of( rsfPair.getKey(), rsfPair.getValue().getUninterruptibly() ) )
                .map( rsPair -> RowAdapters.entityIdFQNPair( rsPair.getValue(),
                        authorizedPropertyTypesForEntitySets.get( rsPair.getKey() ),
                        mapper ) )
                .forEach( pair -> {
                   result.putAll( pair.getValue() );
                   pair.getKey().entries().forEach( entry -> {
                       try {
                        indexResult.put( entry.getKey(), ObjectMappers.getJsonMapper().writeValueAsString( entry.getValue() ) );
                    } catch ( JsonProcessingException e ) {
                        logger.debug( "unable to write property field for indexing" );
                    }
                   } );
                });

        eventBus.post( new EntityDataCreatedEvent( linkedEntitySetId, linkedKey.getKey().toString(), indexResult ) );
        return result;
    }
}
