package com.kryptnostic.types.services;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.TypePK;
import com.kryptnostic.datastore.edm.Queries;
import com.kryptnostic.datastore.edm.Queries.ParamNames;
import com.kryptnostic.datastore.util.Util;

public class EntitiyStorageClient {
    private static final Logger                  logger = LoggerFactory
            .getLogger( EntitiyStorageClient.class );
    // private final IMap<String, FullQualifiedName> entitySets;
    // private final IMap<FullQualifiedName, EntitySchema> entitySchemas;
    private final EdmManager                     dms;
    private final CassandraTableManager          tableManager;
    private final Session                        session;
    

    public EntitiyStorageClient(
            String keyspace,
            HazelcastInstance hazelcast,
            EdmManager dms,
            Session session,
            CassandraTableManager tableManager,
            CassandraStorage storage,
            MappingManager mm ) {
        initializePreparedStatements( dms );
        this.dms = dms;
        this.tableManager = tableManager;
        this.session = session;
    }


    public EntityCollection readEntitySetData( EdmEntitySet edmEntitySet ) throws ODataApplicationException {
        // exec.submit( new ConductorCallable )
        // actually, this is only required if we have more than one Entity Sets
        // if(edmEntitySet.getName().equals(DemoEdmProvider.ES_PRODUCTS_NAME)){
        // return getProducts();
        // }

        // TODO: RPC to Spark to load data.
        return null;
    }

    public Entity readEntityData( EdmEntitySet edmEntitySet, List<UriParameter> keyParams )
            throws ODataApplicationException {

        EdmEntityType edmEntityType = edmEntitySet.getEntityType();

        // actually, this is only required if we have more than one Entity Type
        // if(edmEntityType.getName().equals(DemoEdmProvider.ET_PRODUCT_NAME)){
        //// return getProduct(edmEntityType, keyParams);
        // }

        return null;
    }

    public Entity createEntityData( UUID aclId, UUID syncId, EdmEntitySet edmEntitySet, Entity requestEntity ) {
        FullQualifiedName entityFqn = edmEntitySet.getEntityType().getFullQualifiedName();
        Preconditions.checkArgument(
                dms.isExistingEntitySet( entityFqn, edmEntitySet.getName() ),
                "Cannot add data to non-existant entity set." );
        UUID objectId = writeEntity( DatastoreConstants.KEYSPACE,
                tableManager.getTypenameForEntityType( edmEntitySet.getEntityType().getFullQualifiedName() ),
                aclId,
                syncId,
                ImmutableSet.of( edmEntitySet.getName() ) );
        EntityType entityType = dms.getEntityType( entityFqn.getNamespace(), entityFqn.getName() );
        writeProperties( entityType,
                DatastoreConstants.KEYSPACE,
                objectId,
                aclId,
                syncId,
                requestEntity.getProperties() );
        return requestEntity;
    }

    private UUID writeEntity( String keyspace, String typename, UUID aclId, UUID syncId, Set<String> entitySetNames ) {
        return writeEntity( aclId, syncId, entitySetNames, "INSERT INTO " + keyspace + "." + typename );
    }

    private UUID writeEntity( UUID aclId, UUID syncId, Set<String> entitySetNames, String insertEntityQuery ) {
        UUID objectId = UUID.randomUUID();
        String query = insertEntityQuery + Queries.INSERT_ENTITY_CLAUSES;
        while ( Util.wasLightweightTransactionApplied( session.execute( query,
                ImmutableMap.of( ParamNames.OBJ_ID,
                        objectId,
                        ParamNames.ACL_ID,
                        aclId,
                        ParamNames.SYNC_IDS,
                        ImmutableSet.of( syncId ),
                        ParamNames.ENTITY_SETS,
                        entitySetNames ) ) ) ) {
            objectId = UUID.randomUUID();
        }
        return objectId;
    }

    private List<ResultSet> writeProperties(
            EntityType entityType,
            String keyspace,
            UUID objectId,
            UUID aclId,
            UUID syncId,
            List<Property> properties ) {
        Set<FullQualifiedName> propertyTypes = entityType.getProperties();
        SetMultimap<String, FullQualifiedName> nameLookup = HashMultimap.create();

        propertyTypes.forEach( fqn -> nameLookup.put( fqn.getName(), fqn ) );
        return properties.parallelStream().map( property -> {
            String propertyType = property.getType();

            FullQualifiedName fqn = null;

            if ( propertyType.contains( "." ) ) {
                fqn = new FullQualifiedName( propertyType );
            } else {
                Set<FullQualifiedName> possibleTypes = nameLookup.get( propertyType );
                if ( possibleTypes.size() == 1 ) {
                    fqn = possibleTypes.iterator().next();
                } else {
                    logger.error( "Unable to resolve property {} for entity type {}... possible properties: {}",
                            property,
                            entityType,
                            possibleTypes );
                }
            }

            if ( fqn == null ) {
                logger.error( "Invalid property {} for entity type {}... skipping", property, entityType );
                return null;
            }

            String propertyTable = tableManager.getTypenameForPropertyType( fqn );

            Statement insertQuery = QueryBuilder.insertInto( DatastoreConstants.KEYSPACE, propertyTable )
                    .value( "objectId", objectId )
                    .value( "aclId", aclId ).value( "syncIds", ImmutableSet.of( syncId ) )
                    .value( "value", property.getValue() );
            return session.executeAsync( insertQuery );

        } ).filter( rsf -> rsf != null ).map( rsf -> {
            try {
                return rsf.get();
            } catch ( InterruptedException | ExecutionException e ) {
                logger.error( "Failed writing property!", e );
                // Should probably wrap in uncheckced exception and bubble up.
                return null;
            }
        } ).filter( rs -> rs != null ).collect( Collectors.toList() );
    }
}
