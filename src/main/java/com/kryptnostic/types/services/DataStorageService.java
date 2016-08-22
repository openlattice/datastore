package com.kryptnostic.types.services;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.conductor.rpc.Employee;
import com.kryptnostic.conductor.rpc.Lambdas;
import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;
import com.kryptnostic.datastore.edm.Queries;
import com.kryptnostic.datastore.edm.Queries.ParamNames;
import com.kryptnostic.datastore.util.Util;

public class DataStorageService implements Serializable {
    private static final long                      serialVersionUID = 6909531250591139837L;
    private static final Logger                    logger           = LoggerFactory
            .getLogger( DataStorageService.class );
    // private final IMap<String, FullQualifiedName> entitySets;
    // private final IMap<FullQualifiedName, EntitySchema> entitySchemas;
    private transient final IExecutorService       exec;
    private transient final DurableExecutorService durable;
    private transient final EdmManager             dms;
    private transient final CassandraStorage       storage;
    private transient final CassandraTableManager  tableManager;
    private transient final Session                session;

    public DataStorageService(
            HazelcastInstance hazelcast,
            EdmManager dms,
            Session session,
            CassandraTableManager tableManager,
            CassandraStorage storage ) {
        exec = hazelcast.getExecutorService( "default" );
        durable = hazelcast.getDurableExecutorService( "default" );
        this.dms = dms;
        this.storage = storage;
        this.tableManager = tableManager;
        this.session = session;
    }

    public List<Employee> getEmployees() {
        try {
            return exec.submit( Lambdas.getEmployees() ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to retrieve employees.", e );
            return null;
        }
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

    public Entity createEntityData( UUID syncId, UUID aclId, EdmEntitySet edmEntitySet, Entity requestEntity ) {
        Preconditions.checkArgument(
                dms.isExistingEntitySet( edmEntitySet.getEntityType().getFullQualifiedName(), edmEntitySet.getName() ),
                "Cannot add data to non-existant entity set." );
        writeEntity( aclId,
                syncId,
                ImmutableSet.of( edmEntitySet.getName() ),
                DatastoreConstants.KEYSPACE,
                tableManager.getTypenameForType( edmEntitySet.getEntityType().getFullQualifiedName() ) );
        // writeProperties( requestEntity.getProperties() );
        return null;
    }

    private UUID writeEntity( UUID aclId, UUID syncId, Set<String> entitySetNames, String keyspace, String typename ) {
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

    private void writeProperties( UUID dataSource, UUID aclId, List<Property> properties ) {}

}
