package com.kryptnostic.types.services;

import java.util.Map;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.collect.ImmutableMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.conductor.rpc.odata.Schema;
import com.kryptnostic.conductor.rpc.odata.TypePK;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.edm.Queries;
import com.kryptnostic.datastore.util.Util;

public class CassandraTableManager {
    private final Session                                   session;
    IMap<TypePK, PropertyType>                              propertyTypes;
    IMap<TypePK, EntityType>                                entityTypes;

    private final Map<FullQualifiedName, PreparedStatement> propertyTypeInsertStatements;
    private final Map<FullQualifiedName, PreparedStatement> entityTypeInsertStatements;

    private final Mapper<EntityType>                        entityTypeMapper;
    private final Mapper<PropertyType>                      propertyTypeMapper;
    private final String                                    keyspace;

    public CassandraTableManager(
            HazelcastInstance hazelcast,
            String keyspace,
            Session session,
            MappingManager mm ) {
        createKeyspaceSparksIfNotExists( session );
        createSchemasTableIfNotExists( session );
        createEntityTypesTableIfNotExists( session );
        createPropertyTypesTableIfNotExists( session );
        createEntitySetTableIfNotExists( session );

        this.session = session;
        this.keyspace = keyspace;
        this.entityTypeMapper = mm.mapper( EntityType.class );
        this.propertyTypeMapper = mm.mapper( PropertyType.class );
    }

    public void registerSchema( Schema schema ) {
        schema.getEntityTypes().forEach( et -> {
            entityTypeInsertStatements.put( et.getFullQualifiedName(),
                    session.prepare( QueryBuilder.insertInto( keyspace, getTypenameForEntityType( et ) )
                            .value( CommonColumns.OBJECTID.toString(), QueryBuilder.bindMarker() )
                            .value( CommonColumns.ACLID.toString(), QueryBuilder.bindMarker() )
                            .value( CommonColumns.CLOCK.toString(), QueryBuilder.bindMarker() )
                            .value( CommonColumns.ENTITYSETS.toString(), QueryBuilder.bindMarker() )
                            .value( CommonColumns.SYNCIDS.toString(), QueryBuilder.bindMarker() ) ) );

        } );
        schema.getPropertyTypes().forEach( pt -> {
            entityTypeInsertStatements.put( pt.getFullQualifiedName(),
                    session.prepare( QueryBuilder.insertInto( keyspace, getTypenameForPropertyType( pt ) )
                            .value( CommonColumns.OBJECTID.toString(), QueryBuilder.bindMarker() )
                            .value( CommonColumns.ACLID.toString(), QueryBuilder.bindMarker() )
                            .value( CommonColumns.VALUE
                                    .getType( c -> CassandraEdmMapping.getCassandraType( pt.getDatatype() ) )
                                    .toString(), QueryBuilder.bindMarker() )
                            .value( CommonColumns.ENTITYSETS.toString(), QueryBuilder.bindMarker() )
                            .value( CommonColumns.SYNCIDS.toString(), QueryBuilder.bindMarker() ) ) );

        } );
    }

    public String createEntityTypeTable( EntityType entityType ) {
        String typename;
        String entityTableQuery;
        do {
            typename = getEntityTypename();
            entityTableQuery = String.format( keyspace,
                    Queries.CREATE_ENTITY_TABLE,
                    typename );
        } while ( !Util.wasLightweightTransactionApplied( session.execute( entityTableQuery ) ) );
        // Loop until table creation succeeds.
        return typename;
    }

    public String createPropertyTypeTable( PropertyType propertyType ) {
        String typename;
        String propertyTableQuery;
        do {
            typename = getTypenameForPropertyType( propertyType.getNamespace(), propertyType.getName() );
            propertyTableQuery = String.format( Queries.CREATE_PROPERTY_TABLE,
                    keyspace,
                    typename,
                    CassandraEdmMapping.getCassandraTypeName( propertyType.getDatatype() ) );
        } while ( !Util.wasLightweightTransactionApplied( session.execute( propertyTableQuery ) ) );
        // Loop until table creation succeeds.
        return typename;
    }

    public static String getPropertyTypename() {
        return "p_" + RandomStringUtils.randomAlphanumeric( 24 ) + "_properties";
    }

    public static String getEntityTypename() {
        return "e_" + RandomStringUtils.randomAlphanumeric( 24 ) + "objects";
    }

    public void deleteEntityTypeTable( String namespace, String entityName ) {
        // We should mark tables for deletion-- we lose historical information if we hard delete properties.
        /*
         * Use Accessor interface to look up objects and retrieve typename corresponding to table to delete.
         */
        throw new NotImplementedException( "Blame MTR" );
    }

    public void deletePropertyTypeTable( String namespace, String propertyName ) {
        throw new NotImplementedException( "Blame MTR" );
    }

    public String getTypenameForEntityType( EntityType entityType ) {
        return getTypenameForEntityType( entityType.getNamespace(), entityType.getName() );
    }

    public String getTypenameForEntityType( FullQualifiedName fullQualifiedName ) {
        return getTypenameForEntityType( fullQualifiedName.getNamespace(), fullQualifiedName.getName() );
    }

    public String getTypenameForEntityType( String namespace, String name ) {
        // TODO: Extract strings... so many queries
        Row r = session.execute(
                "select * from sparks." + DatastoreConstants.ENTITY_TYPES_TABLE + " where namespace=:ns AND name:=t",
                ImmutableMap.of( "ns", namespace, "t", name ) )
                .one();
        if ( r == null ) {
            return getEntityTypename();
        }
        return r.getString( "typename" );
    }

    public String getTypenameForPropertyType( PropertyType propertyType ) {
        return getTypenameForPropertyType( propertyType.getNamespace(), propertyType.getName() );
    }

    public String getTypenameForPropertyType( FullQualifiedName fullQualifiedName ) {
        return getTypenameForPropertyType( fullQualifiedName.getNamespace(), fullQualifiedName.getName() );
    }

    public String getKeyspace() {
        return keyspace;
    }

    private String getTypenameForPropertyType( String namespace, String name ) {
        // TODO: Extract strings... so many queries
        // TODO: Prepared statements
        Row r = session.execute(
                "select * from sparks." + DatastoreConstants.PROPERTY_TYPES_TABLE + " where namespace=:ns AND name=:t",
                ImmutableMap.of( "ns", namespace, "t", name ) )
                .one();
        if ( r == null ) {
            return null;
        }
        return r.getString( "typename" );
    }

    private static void createKeyspaceSparksIfNotExists( Session session ) {
        session.execute( Queries.CREATE_KEYSPACE );
    }

    private static void createSchemasTableIfNotExists( Session session ) {
        session.execute( Queries.CREATE_SCHEMAS_TABLE );
    }

    private static void createEntitySetTableIfNotExists( Session session ) {
        session.execute( Queries.CREATE_ENTITY_SETS_TABLE );
        session.execute( Queries.CREATE_INDEX_ON_NAME );
    }

    private static void createEntityTypesTableIfNotExists( Session session ) {
        session.execute( Queries.CREATE_ENTITY_TYPES_TABLE );
    }

    private void createPropertyTypesTableIfNotExists( Session session ) {
        session.execute( Queries.CREATE_PROPERTY_TYPES_TABLE );
    }
}
