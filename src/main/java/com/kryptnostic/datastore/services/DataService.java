package com.kryptnostic.datastore.services;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.hazelcast.core.IMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.hazelcast.core.HazelcastInstance;
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
        this.urlToIntegrationScripts = hazelcast.getMap( "url_to_scripts" );
    }

    public Map<String, Object> getObject( UUID objectId ) {
//        tableManager.getTablenameForPropertyValues( )
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
