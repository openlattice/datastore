package com.kryptnostic.types.pods;

import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;
import com.kryptnostic.rhizome.registries.ObjectMapperRegistry;
import com.kryptnostic.types.services.CassandraStorage;
import com.kryptnostic.types.services.CassandraTableManager;
import com.kryptnostic.types.services.DataModelService;
import com.kryptnostic.types.services.DataStorageService;
import com.kryptnostic.types.services.EdmManager;

@Configuration
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
        return new DataModelService( session, mappingManager(), tableManager() );
    }

    @Bean
    public CassandraStorage storage() {
        return mappingManager().createAccessor( CassandraStorage.class );
    }

    @Bean
    public DataStorageService dataStorageService() {
        return new DataStorageService( hazelcastInstance, dataModelService(), session, tableManager(), storage() );
    }

}
