package com.kryptnostic.types.services;

import java.util.List;
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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.rpc.odata.EntityType;

public class EntityStorageClient {
    private static final Logger         logger = LoggerFactory
            .getLogger( EntityStorageClient.class );
    // private final IMap<String, FullQualifiedName> entitySets;
    // private final IMap<FullQualifiedName, EntitySchema> entitySchemas;
    private final EdmManager            dms;
    private final CassandraTableManager tableManager;
    private final Session               session;
    private final String                keyspace;

    public EntityStorageClient(
            String keyspace,
            HazelcastInstance hazelcast,
            EdmManager dms,
            Session session,
            CassandraTableManager tableManager,
            CassandraStorage storage,
            MappingManager mm ) {
        this.dms = dms;
        this.tableManager = tableManager;
        this.session = session;
        this.keyspace = keyspace;
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
        return createEntityData( aclId, syncId, edmEntitySet.getName(), entityFqn, requestEntity );
    }

    public Entity createEntityData(
            UUID aclId,
            UUID syncId,
            String entitySetName,
            FullQualifiedName entityFqn,
            Entity requestEntity ) {
        PreparedStatement query = tableManager.getInsertEntityPreparedStatement( entityFqn );
        UUID objectId = UUID.randomUUID();
        BoundStatement boundQuery = query.bind( objectId,
                aclId,
                ImmutableSet.of( entitySetName ),
                ImmutableList.of( syncId ) );
        session.execute( boundQuery );

        EntityType entityType = dms.getEntityType( entityFqn.getNamespace(), entityFqn.getName() );
        writeProperties( entityType,
                keyspace,
                objectId,
                aclId,
                syncId,
                requestEntity.getProperties() );
        return requestEntity;
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

            PreparedStatement insertQuery = tableManager.getUpdatePropertyPreparedStatement( fqn );
            return session.executeAsync(
                    insertQuery.bind( objectId, aclId, property.getValue(), ImmutableList.of( syncId ) ) );

        } ).map( rsf -> {
            if ( rsf == null ) {
                return null;
            }
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
