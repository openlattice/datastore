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
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.events.EntityDataCreatedEvent;
import com.dataloom.edm.PropertyType;
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
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.eventbus.EventBus;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.RowAdapters;
import com.kryptnostic.rhizome.cassandra.CassandraTableBuilder;

public class CassandraDataManager {
    
    @Inject
    private EventBus                                eventBus;
    
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
        return Iterables.transform( entityRows,
                rs -> RowAdapters.entity( rs, authorizedPropertyTypes, mapper ) )::iterator;
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

            results.add(
                    session.executeAsync( writeIdLookupQuery.bind().setUUID( CommonColumns.SYNCID.cql(), syncId )
                            .setUUID( CommonColumns.ENTITY_SET_ID.cql(), entitySetId )
                            .setString( CommonColumns.ENTITYID.cql(), entity.getKey() ) ) );

            SetMultimap<UUID, Object> propertyValues = entity.getValue();
            
            Map<UUID, String> authorizedPropertyValues = Maps.newHashMap();
            //Stream<Entry<UUID, Object>> authorizedPropertyValues = propertyValues.entries().stream().filter( entry -> authorizedProperties.contains( entry.getKey() ) );
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
                        //TODO: wtf move this
                        try {
                            authorizedPropertyValues.put( entry.getKey(), ObjectMappers.getJsonMapper().writeValueAsString( entry.getValue() ) );
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
