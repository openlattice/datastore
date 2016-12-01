package com.kryptnostic.datastore.pods;

import javax.inject.Inject;

import com.kryptnostic.datastore.services.*;
import digital.loom.rhizome.authentication.Auth0Pod;
import digital.loom.rhizome.configuration.auth0.Auth0Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.dataloom.edm.internal.DatastoreConstants;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.datastore.cassandra.CassandraStorage;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.kryptnostic.rhizome.registries.ObjectMapperRegistry;

@Configuration
@Import( { CassandraPod.class, Auth0Pod.class } )
public class DatastoreServicesPod {

    @Inject
    private HazelcastInstance  hazelcastInstance;

    @Inject
    private Session            session;

    @Inject
    private Auth0Configuration auth0Configuration;

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
    public PermissionsService permissionsService() {
        return new PermissionsService( session, mappingManager(), tableManager() );
    }

    @Bean
    public ActionAuthorizationService authzService() {
        return new ActionAuthorizationService( permissionsService() );
    }

    @Bean
    public EdmManager dataModelService() {
        return new EdmService( session, hazelcastInstance, mappingManager(), tableManager(), permissionsService() );
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
    public DataService dataService() {
        return new DataService(
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

    @Bean
    public UserDirectoryService userDirectoryService() {
        return new UserDirectoryService( auth0Configuration.getToken() );
    }

}
