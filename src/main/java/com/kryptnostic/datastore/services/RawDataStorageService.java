package com.kryptnostic.datastore.services;

import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.datastore.cassandra.CassandraStorage;

public class RawDataStorageService {
    private static final Logger         logger = LoggerFactory
            .getLogger( RawDataStorageService.class );
    // private final IMap<String, FullQualifiedName> entitySets;
    // private final IMap<FullQualifiedName, EntitySchema> entitySchemas;
    private final EdmManager            dms;
    private final CassandraTableManager tableManager;
    private final Session               session;
    private final String                keyspace;

    public RawDataStorageService(
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

    public Map<String, Object> getObject( UUID objectId ) {
//        tableManager.getTablenameForPropertyValues( )
        return null;
    }

}
