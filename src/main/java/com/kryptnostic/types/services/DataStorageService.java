package com.kryptnostic.types.services;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.conductor.rpc.Employee;
import com.kryptnostic.conductor.rpc.Lambdas;

public class DataStorageService implements Serializable {
    private static final long                      serialVersionUID = 6909531250591139837L;
    private static final Logger                    logger           = LoggerFactory
            .getLogger( DataStorageService.class );
    // private final IMap<String, FullQualifiedName> entitySets;
    // private final IMap<FullQualifiedName, EntitySchema> entitySchemas;
    private transient final IExecutorService       exec;
    private transient final DurableExecutorService durable;
    

    public DataStorageService( HazelcastInstance hazelcast ) {
        exec = hazelcast.getExecutorService( "default" );
        durable = hazelcast.getDurableExecutorService( "default" );
        exec.submit( (Runnable & Serializable) () -> System.out.println( "UNSTOPPABLE MTR" ) );
        logger.info( "Loaded employees: {}", getEmployees() );
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

    public Entity createEntityData( EdmEntitySet edmEntitySet, Entity requestEntity ) {
        // edmEntitySet.getEntityType().getKeyPropertyRefs().forEach( kp -> kp.getProperty().getType(). );
        // TODO Auto-generated method stub
        
        return null;
    }

}
