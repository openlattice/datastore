package com.kryptnostic.types.services;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomStringUtils;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.kryptnostic.datastore.util.CassandraEdmMapping;
import com.kryptnostic.datastore.util.DatastoreConstants.Queries;
import com.kryptnostic.datastore.util.Util;
import com.kryptnostic.types.EntityType;
import com.kryptnostic.types.PropertyType;

public class CasasndraTableManager {
    private final Session              session;
    private final Mapper<EntityType>   entityTypeMapper;
    private final Mapper<PropertyType> propertyTypeMapper;
    private final String               keyspace;

    public CasasndraTableManager(
            String keyspace,
            Session session,
            Mapper<EntityType> entityTypeMapper,
            Mapper<PropertyType> propertyTypeMapper ) {
        this.session = session;
        this.keyspace = keyspace;
        this.entityTypeMapper = entityTypeMapper;
        this.propertyTypeMapper = propertyTypeMapper;
    }

    public String createEntityTypeTable( EntityType entityType ) {
        String typename;
        String entityTableQuery;
        do {
            typename = getEntityTypename( entityType );
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
            typename = getPropertyTypename( propertyType );
            propertyTableQuery = String.format( keyspace,
                    Queries.CREATE_PROPERTY_TABLE,
                    typename,
                    CassandraEdmMapping.getCassandraTypeName( propertyType.getDatatype() ) );
        } while ( !Util.wasLightweightTransactionApplied( session.execute( propertyTableQuery ) ) );
        // Loop until table creation succeeds.
        return typename;
    }

    private String getPropertyTypename( PropertyType propertyType ) {
        return RandomStringUtils.randomAlphanumeric( 24 ) + "_properties";
    }

    private String getEntityTypename( EntityType entityType ) {
        return RandomStringUtils.randomAlphanumeric( 24 ) + "objects";
    }

    public void deleteEntityTypeTable( String namespace, String entityName ) {
        //We should mark tables for deletion-- we lose historical information if we hard delete properties.
        /*
         * Use Accessor interface to look up objects and retrieve typename corresponding to table to delete. 
         */
        throw new NotImplementedException( "Blame MTR" );
    }

    public void deletePropertyTypeTable( String namespace, String propertyName ) {
        throw new NotImplementedException( "Blame MTR" );
    }

}
