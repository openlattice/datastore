package com.kryptnostic.datastore.data;

import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.dataloom.mappers.ObjectMappers;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kryptnostic.datastore.services.CassandraDataManager;
import com.kryptnostic.rhizome.pods.CassandraPod;

@Configuration
@Import( { CassandraPod.class } )
public class DataManagerTestPod {

    @Inject
    private Session session;

    @Bean
    public ObjectMapper defaultObjectMapper() {
        return ObjectMappers.getJsonMapper();
    }

    @Bean
    public CassandraDataManager cassandraDataManager() {
        return new CassandraDataManager( session, defaultObjectMapper() );
    }
}
