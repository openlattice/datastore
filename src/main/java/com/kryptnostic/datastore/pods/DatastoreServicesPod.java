package com.kryptnostic.datastore.pods;

import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.dataloom.edm.internal.DatastoreConstants;
import com.dataloom.edm.schemas.SchemaQueryService;
import com.dataloom.edm.schemas.cassandra.CassandraSchemaQueryService;
import com.dataloom.edm.schemas.manager.HazelcastSchemaManager;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.datastore.cassandra.CassandraStorage;
import com.kryptnostic.datastore.services.ActionAuthorizationService;
import com.kryptnostic.datastore.services.CassandraEntitySetManager;
import com.kryptnostic.datastore.services.CassandraTableManager;
import com.kryptnostic.datastore.services.DataService;
import com.kryptnostic.datastore.services.DatasourceManager;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.EdmService;
import com.kryptnostic.datastore.services.ODataStorageService;
import com.kryptnostic.datastore.services.PermissionsService;
import com.kryptnostic.datastore.services.UserDirectoryService;
import com.kryptnostic.datastore.util.PermissionsResultsAdapter;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.kryptnostic.rhizome.registries.ObjectMapperRegistry;

import digital.loom.rhizome.authentication.Auth0Pod;
import digital.loom.rhizome.configuration.auth0.Auth0Configuration;

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
                session );
    }

    @Bean
    public PermissionsService permissionsService() {
        return new PermissionsService( session, mappingManager(), tableManager() );
    }

    @Bean
    public SchemaQueryService schemaQueryService() {
        return new CassandraSchemaQueryService(DatastoreConstants.KEYSPACE, session );
    }
    @Bean
    public CassandraEntitySetManager entitySetManager() {
        return new CassandraEntitySetManager( session, DatastoreConstants.KEYSPACE );
    }
    
    @Bean
    public HazelcastSchemaManager schemaManager() {
        return new HazelcastSchemaManager( DatastoreConstants.KEYSPACE, hazelcastInstance, schemaQueryService() );
    }

    @Bean
    public ActionAuthorizationService authzService() {
        return new ActionAuthorizationService( permissionsService() );
    }

    @Bean
    public EdmManager dataModelService() {
        return new EdmService( DatastoreConstants.KEYSPACE, session, hazelcastInstance, tableManager(), entitySetManager(), schemaManager(), permissionsService() );
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

    @Bean
    public PermissionsResultsAdapter permissionsResultsAdapter() {
        return new PermissionsResultsAdapter( hazelcastInstance, userDirectoryService() );
    }

}
