package com.kryptnostic.datastore.services;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.kryptnostic.conductor.rpc.*;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.cassandra.CassandraStorage;

public class DataService {
    private static final Logger         logger = LoggerFactory
            .getLogger( DataService.class );
    // private final IMap<String, FullQualifiedName> entitySets;
    // private final IMap<FullQualifiedName, EntitySchema> entitySchemas;
    private final EdmManager            dms;
    private final CassandraTableManager tableManager;
    private final Session               session;
    private final String                keyspace;
    private final DurableExecutorService executor;
    private final IMap<String, String>  urlToIntegrationScripts;

    public DataService(
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
        this.urlToIntegrationScripts = hazelcast.getMap( "url_to_scripts" );
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
    public Iterable< SetMultimap<FullQualifiedName, Object> > readAllEntitiesOfType( FullQualifiedName fqn ) {
        try {
            QueryResult result = executor
                    .submit( ConductorCall
                            .wrap( Lambdas.getEntities( fqn ) ) )
                    .get();
            //Get properties of the entityType
            EntityType entityType = dms.getEntityType( fqn );
            Set<PropertyType> properties = new HashSet<PropertyType>();
            entityType.getProperties().forEach(
                    property -> properties.add( dms.getPropertyType( property ) )
            );

            return Iterables.transform( result, row -> ResultSetAdapterFactory.mapRowToObject( row, properties ) );

        } catch ( InterruptedException | ExecutionException e ) {
            e.printStackTrace();
        }
        ;
        return null;
    }

    public Map<String, String> getAllIntegrationScripts() {
        return urlToIntegrationScripts;
    }

    public Map<String, String> getIntegrationScriptForUrl(Set<String> urls){
        return urlToIntegrationScripts.getAll( urls );
    }

    public void createIntegrationScript( Map<String, String> integrationScripts ){
        urlToIntegrationScripts.putAll( integrationScripts );
    }

}
