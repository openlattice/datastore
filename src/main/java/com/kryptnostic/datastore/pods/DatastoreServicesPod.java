package com.kryptnostic.datastore.pods;

import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;
import com.kryptnostic.datastore.services.CassandraStorage;
import com.kryptnostic.datastore.services.CassandraTableManager;
import com.kryptnostic.datastore.services.DatasourceManager;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.EdmService;
import com.kryptnostic.datastore.services.EntityStorageClient;
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
                hazelcastInstance,
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
    public EntityStorageClient dataStorageService() {
        return new EntityStorageClient(
                DatastoreConstants.KEYSPACE,
                hazelcastInstance,
                dataModelService(),
                session,
                tableManager(),
                storage(),
                mappingManager() );
    }

    @Bean
    public DatasourceManager datasourceManager() {
        return new DatasourceManager();
    }

}
