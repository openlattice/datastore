package com.kryptnostic.datastore.services;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.conductor.rpc.ConductorCall;
import com.kryptnostic.conductor.rpc.GetAllEntitiesOfTypeLambda;
import com.kryptnostic.conductor.rpc.QueryResult;
import com.kryptnostic.conductor.rpc.ResultSetAdapterFactory;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.datastore.cassandra.CassandraStorage;

public class DataStorageClient {
    private static final Logger         logger = LoggerFactory
            .getLogger( DataStorageClient.class );
    // private final IMap<String, FullQualifiedName> entitySets;
    // private final IMap<FullQualifiedName, EntitySchema> entitySchemas;
    private final EdmManager            dms;
    private final CassandraTableManager tableManager;
    private final Session               session;
    private final String                keyspace;
    private final DurableExecutorService executor;

    public DataStorageClient(
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
        // TODO: Configure executor service.
        this.executor = hazelcast.getDurableExecutorService( "default" );
    }

    public Map<String, Object> getObject( UUID objectId ) {
//        tableManager.getTablenameForPropertyValues( )
        return null;
    }

    /**
     * 
     * @param fqn FullQualifiedName of Entity Type
     * @return Iterable of all properties of each entity of the correct type, in the form of SetMultimap<Property Names, Value>
     */
    public Iterable< SetMultimap<FullQualifiedName, Object> > readAllEntitiesOfType( FullQualifiedName fqn ){
    	try{
    		QueryResult result = executor
                    .submit( ConductorCall
                            .wrap( new GetAllEntitiesOfTypeLambda( fqn ) ) )
                    .get();
    		//Get properties of the entityType
        	EntityType entityType = dms.getEntityType( fqn );
        	Set<String> propertyTypenames = new HashSet<String>();
        	entityType.getProperties().forEach( 
        				property -> propertyTypenames.add( dms.getPropertyType( property ).getName() ) 
        			);
        	//Get Map from typename to FullQualifiedName of the property types
    		Map<String, FullQualifiedName> map = tableManager.getFullQualifiedNamesForTypenames(propertyTypenames);
    		
    		return Iterables.transform( result, ResultSetAdapterFactory.toSetMultimap(map) );
    		
    	} catch ( InterruptedException | ExecutionException e ) {
        	e.printStackTrace();
        };
    	return null;    	
    }

}
