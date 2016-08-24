package com.kryptnostic.types.services;

import java.util.Map;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.MappingManager;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.conductor.rpc.odata.Schema;
import com.kryptnostic.conductor.rpc.odata.TypePK;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import com.kryptnostic.datastore.cassandra.CassandraTableBuilder;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.edm.Queries;
import com.kryptnostic.datastore.util.Util;

import jersey.repackaged.com.google.common.base.Preconditions;
import jersey.repackaged.com.google.common.collect.Maps;

public class CassandraTableManager {
    private final Session                                   session;
    IMap<TypePK, PropertyType>                              propertyTypes;
    IMap<TypePK, EntityType>                                entityTypes;

    private final Map<FullQualifiedName, PreparedStatement> propertyTypeUpdateStatements;
    private final Map<FullQualifiedName, PreparedStatement> entityTypeInsertStatements;
    private final Map<FullQualifiedName, PreparedStatement> entityTypeUpdateStatements;
    private final String                                    keyspace;

    private final PreparedStatement                         getTypenameForEntityType;
    private final PreparedStatement                         getTypenameForPropertyType;
    private final PreparedStatement                         countProperty;

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
        this.propertyTypeUpdateStatements = Maps.newConcurrentMap();
        this.entityTypeInsertStatements = Maps.newConcurrentMap();
        this.entityTypeUpdateStatements = Maps.newConcurrentMap();

        this.getTypenameForEntityType = session.prepare( QueryBuilder
                .select()
                .from( keyspace, DatastoreConstants.ENTITY_TYPES_TABLE )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.toString(),
                        QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.toString(),
                        QueryBuilder.bindMarker() ) ) );
        this.getTypenameForPropertyType = session.prepare( QueryBuilder
                .select()
                .from( keyspace, DatastoreConstants.PROPERTY_TYPES_TABLE )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.toString(),
                        QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.toString(),
                        QueryBuilder.bindMarker() ) ) );

        this.countProperty = session.prepare( QueryBuilder
                .select().countAll()
                .from( keyspace, DatastoreConstants.PROPERTY_TYPES_TABLE )
                .where( QueryBuilder.eq( CommonColumns.NAMESPACE.toString(),
                        QueryBuilder.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.NAME.toString(),
                        QueryBuilder.bindMarker() ) ) );

    }

    public void registerSchema( Schema schema ) {
        Preconditions.checkArgument( schema.getEntityTypeFqns().size() == schema.getEntityTypes().size(),
                "Schema is out of sync." );
        schema.getEntityTypes().forEach( et -> {
            // TODO: Solve ID generation
            /*
             * While unlikely it's possible to have a UUID collision when creating an object. Two possible solutions:
             * (1) Use Hazelcast and perform a read prior to every write (2) Maintain a self-refreshing in-memory pool
             * of available UUIDs that shifts the reads to times when cassandra is under less stress. Option (2) with a
             * fall back to random UUID generation when pool is exhausted seems like an efficient bet.
             */
            entityTypeInsertStatements.put( et.getFullQualifiedName(),
                    session.prepare( QueryBuilder
                            .insertInto( keyspace, getTypenameForEntityType( et ) )
                            .value( CommonColumns.OBJECTID.toString(), QueryBuilder.bindMarker() )
                            .value( CommonColumns.ACLID.toString(), QueryBuilder.bindMarker() )
                            .value( CommonColumns.CLOCK.toString(), QueryBuilder.now() )
                            .value( CommonColumns.ENTITYSETS.toString(), QueryBuilder.bindMarker() )
                            .value( CommonColumns.SYNCIDS.toString(), QueryBuilder.bindMarker() ) ) );

            entityTypeUpdateStatements.put( et.getFullQualifiedName(),
                    session.prepare( QueryBuilder.update( keyspace, getTypenameForEntityType( et ) )
                            .where( QueryBuilder.eq( CommonColumns.OBJECTID.toString(), QueryBuilder.bindMarker() ) )
                            .and( QueryBuilder.eq( CommonColumns.ACLID.toString(), QueryBuilder.bindMarker() ) )
                            .and( QueryBuilder.eq( CommonColumns.VALUE.toString(), QueryBuilder.bindMarker() ) )
                            .with( QueryBuilder.set( CommonColumns.OBJECTID.toString(), QueryBuilder.bindMarker() ) )
                            .and( QueryBuilder.set( CommonColumns.ACLID.toString(), QueryBuilder.bindMarker() ) )
                            .and( QueryBuilder.set( CommonColumns.CLOCK.toString(), QueryBuilder.bindMarker() ) )
                            .and( QueryBuilder.appendAll( CommonColumns.ENTITYSETS.toString(),
                                    QueryBuilder.bindMarker() ) )
                            .and( QueryBuilder.set( CommonColumns.SYNCIDS.toString(), QueryBuilder.bindMarker() ) ) ) );

        } );

        schema.getPropertyTypes().forEach( pt -> {
            propertyTypeUpdateStatements.put( pt.getFullQualifiedName(),
                    session.prepare( QueryBuilder.update( keyspace, getTypenameForPropertyType( pt ) )
                            .where( QueryBuilder.eq( CommonColumns.OBJECTID.toString(), QueryBuilder.bindMarker() ) )
                            .and( QueryBuilder.eq( CommonColumns.ACLID.toString(), QueryBuilder.bindMarker() ) )
                            .and( QueryBuilder.eq( CommonColumns.VALUE.toString(), QueryBuilder.bindMarker() ) )
                            .with( QueryBuilder.set( CommonColumns.OBJECTID.toString(), QueryBuilder.bindMarker() ) )
                            .and( QueryBuilder.set( CommonColumns.ACLID.toString(), QueryBuilder.bindMarker() ) )
                            .and( QueryBuilder.set( CommonColumns.VALUE.toString(), QueryBuilder.bindMarker() ) )
                            .and( QueryBuilder.set( CommonColumns.SYNCIDS.toString(), QueryBuilder.bindMarker() ) ) ) );
        } );
    }

    public PreparedStatement getInsertEntityPreparedStatement( EntityType entityType ) {
        return getInsertEntityPreparedStatement( entityType.getFullQualifiedName() );
    }

    public PreparedStatement getInsertEntityPreparedStatement( FullQualifiedName fqn ) {
        return entityTypeInsertStatements.get( fqn );
    }

    public PreparedStatement getUpdateEntityPreparedStatement( EntityType entityType ) {
        return getUpdateEntityPreparedStatement( entityType.getFullQualifiedName() );
    }

    public PreparedStatement getUpdateEntityPreparedStatement( FullQualifiedName fqn ) {
        return entityTypeUpdateStatements.get( fqn );
    }

    public PreparedStatement getUpdatePropertyPreparedStatement( PropertyType propertyType ) {
        return getUpdatePropertyPreparedStatement( propertyType.getFullQualifiedName() );
    }

    public PreparedStatement getUpdatePropertyPreparedStatement( FullQualifiedName fqn ) {
        return propertyTypeUpdateStatements.get( fqn );
    }

    public PreparedStatement getCountPropertyStatement() {
        return countProperty;
    }

    public String createEntityTypeTable( EntityType entityType ) {
        // Ensure that type name doesn't exist
        String typename = getTypenameForEntityType( entityType.getNamespace(), entityType.getName() );
        Preconditions.checkState( StringUtils.isBlank( typename ) );
        CassandraTableBuilder ctb = new CassandraTableBuilder( keyspace, typename )
                .ifNotExists()
                .partitionKey( CommonColumns.OBJECTID, CommonColumns.ACLID )
                .clusteringColumns( CommonColumns.CLOCK )
                .columns( CommonColumns.ENTITYSETS, CommonColumns.SYNCIDS );
        String entityTableQuery = ctb.buildQuery();
        do {
            entityTableQuery = String.format(
                    Queries.CREATE_ENTITY_TABLE,
                    keyspace,
                    typename );
        } while ( !Util.wasLightweightTransactionApplied( session.execute( entityTableQuery ) ) );
        // Loop until table creation succeeds.
        return typename;
    }

    public String createPropertyTypeTable( PropertyType propertyType ) {
        String typename = getTypenameForPropertyType( propertyType.getNamespace(), propertyType.getName() );
        CassandraTableBuilder ctb = new CassandraTableBuilder( keyspace, typename );
        String propertyTableQuery;
        do {
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

    public static String generateEntityTypename() {
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
        Row r = session.execute( this.getTypenameForEntityType.bind( namespace, name ) ).one();
        if ( r == null ) {
            return null;
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
        Row r = session.execute( this.getTypenameForPropertyType.bind( namespace, name ) ).one();
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
