package com.kryptnostic.datastore.pods;

import javax.inject.Inject;

import com.kryptnostic.datastore.services.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;
import com.kryptnostic.datastore.cassandra.CassandraStorage;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.kryptnostic.rhizome.registries.ObjectMapperRegistry;

@Configuration
@Import( { CassandraPod.class } )
public class DatastoreServicesPod {

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private Session           session;

    @Bean
    public ObjectMapper defaultObjectMapper() {
        return ObjectMapperRegistry.getJsonMapper();
    }

    @Bean
    public MappingManager mappingManager() {
        return new MappingManager( session );
    }

    @Bean
    public CassandraTableManager tableManager() {
        return new CassandraTableManager(
                DatastoreConstants.KEYSPACE,
                session,
                mappingManager() );
    }

    @Bean
    public EdmManager dataModelService() {
        return new EdmService( session, mappingManager(), tableManager() );
    }

    @Bean
    public CassandraStorage storage() {
        return mappingManager().createAccessor( CassandraStorage.class );
    }

    @Bean
    public ODataStorageService odataStorageService() {
        return new ODataStorageService(
                DatastoreConstants.KEYSPACE,
                hazelcastInstance,
                dataModelService(),
                session,
                tableManager(),
                storage(),
                mappingManager() );
    }

    @Bean
    public DataService dataService(){
        return new DataService(
                DatastoreConstants.KEYSPACE,
                hazelcastInstance,
                dataModelService(),
                session,
                tableManager(),
                storage(),
                mappingManager()
        );
    }

    @Bean
    public DatasourceManager datasourceManager() {
        return new DatasourceManager();
    }
    
    @Bean
    public PermissionsService PermissionsService() {
        return new PermissionsService( session, tableManager() );
    }

}
