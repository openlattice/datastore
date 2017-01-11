package com.kryptnostic.datastore.data;

import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.dataloom.mappers.ObjectMappers;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kryptnostic.datastore.services.CassandraDataManager;
import com.kryptnostic.datastore.util.CassandraDataManagerUtils;
import com.kryptnostic.rhizome.pods.CassandraPod;

@Configuration
@Import( { CassandraPod.class } )
public class DataManagerTestPod {

    @Inject
    private Session session;

    final String keyspaceName = "dataManagerTest";
    
    @Bean
    public ObjectMapper defaultObjectMapper() {
        return ObjectMappers.getJsonMapper();
    }

    @Bean
    public CassandraDataManagerUtils cassandraDataManagerUtils() {
        return new CassandraDataManagerUtils( defaultObjectMapper() );
    }
    
    @Bean
    public CassandraDataManager cassandraDataManager() {
        //Create a keyspace for test, in case it doesn't exist.
        session.execute( "CREATE KEYSPACE IF NOT EXISTS " + keyspaceName + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};" );
        return new CassandraDataManager( keyspaceName, session, cassandraDataManagerUtils() );
    }
    
    @Bean
    public Runnable dropCassandraTestKeyspace(){
        return () -> session.execute( "DROP KEYSPACE IF EXISTS " + keyspaceName + ";" );
    }

}
