package com.kryptnostic.types.pods;

import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.rhizome.registries.ObjectMapperRegistry;
import com.kryptnostic.types.services.DataModelService;

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
    public DataModelService dataModelService() {
        return new DataModelService( session );
    }

}
