package com.dataloom.datastore.pods;

import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizationQueryService;
import com.dataloom.authorization.EdmAuthorizationHelper;
import com.dataloom.authorization.HazelcastAclKeyReservationService;
import com.dataloom.authorization.HazelcastAuthorizationService;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.data.serializers.FullQualifedNameJacksonSerializer;
import com.dataloom.datastore.services.CassandraDataManager;
import com.dataloom.datastore.services.DatasourceManager;
import com.dataloom.datastore.services.SearchService;
import com.dataloom.directory.UserDirectoryService;
import com.dataloom.edm.properties.CassandraTypeManager;
import com.dataloom.edm.schemas.SchemaQueryService;
import com.dataloom.edm.schemas.cassandra.CassandraSchemaQueryService;
import com.dataloom.edm.schemas.manager.HazelcastSchemaManager;
import com.dataloom.mappers.ObjectMappers;
import com.dataloom.organizations.HazelcastOrganizationService;
import com.dataloom.requests.HazelcastPermissionsRequestsService;
import com.dataloom.requests.PermissionsRequestsManager;
import com.dataloom.requests.PermissionsRequestsQueryService;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.datastore.services.CassandraEntitySetManager;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.EdmService;
import com.kryptnostic.datastore.services.ODataStorageService;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.pods.CassandraPod;

import digital.loom.rhizome.authentication.Auth0Pod;
import digital.loom.rhizome.configuration.auth0.Auth0Configuration;

@Configuration
@Import( { CassandraPod.class, Auth0Pod.class } )
public class DatastoreServicesPod {

    @Inject
    private CassandraConfiguration cassandraConfiguration;

    @Inject
    private HazelcastInstance      hazelcastInstance;

    @Inject
    private Session                session;

    @Inject
    private Auth0Configuration     auth0Configuration;

    @Bean
    public ObjectMapper defaultObjectMapper() {
        ObjectMapper mapper = ObjectMappers.getJsonMapper();
        FullQualifedNameJacksonSerializer.registerWithMapper( mapper );
        FullQualifedNameJacksonDeserializer.registerWithMapper( mapper );
        return mapper;
    }

    @Bean
    public AuthorizationQueryService authorizationQueryService() {
        return new AuthorizationQueryService( cassandraConfiguration.getKeyspace(), session, hazelcastInstance );
    }

    @Bean
    public AuthorizationManager authorizationManager() {
        return new HazelcastAuthorizationService( hazelcastInstance, authorizationQueryService() );
    }

    @Bean
    public SchemaQueryService schemaQueryService() {
        return new CassandraSchemaQueryService( cassandraConfiguration.getKeyspace(), session );
    }

    @Bean
    public CassandraEntitySetManager entitySetManager() {
        return new CassandraEntitySetManager( cassandraConfiguration.getKeyspace(), session, authorizationManager() );
    }

    @Bean
    public HazelcastSchemaManager schemaManager() {
        return new HazelcastSchemaManager(
                cassandraConfiguration.getKeyspace(),
                hazelcastInstance,
                schemaQueryService() );
    }

    @Bean
    public CassandraTypeManager entityTypeManager() {
        return new CassandraTypeManager( cassandraConfiguration.getKeyspace(), session );
    }

    @Bean
    public EdmManager dataModelService() {
        return new EdmService(
                cassandraConfiguration.getKeyspace(),
                session,
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                entitySetManager(),
                entityTypeManager(),
                schemaManager() );
    }

    @Bean
    public HazelcastAclKeyReservationService aclKeyReservationService() {
        return new HazelcastAclKeyReservationService( hazelcastInstance );
    }

    @Bean
    public ODataStorageService odataStorageService() {
        return new ODataStorageService(
                cassandraConfiguration.getKeyspace(),
                hazelcastInstance,
                dataModelService(),
                session );
    }

    @Bean
    public CassandraDataManager cassandraDataManager() {
        return new CassandraDataManager( session, defaultObjectMapper() );
    }

    @Bean
    public HazelcastOrganizationService organizationsManager() {
        return new HazelcastOrganizationService(
                cassandraConfiguration.getKeyspace(),
                session,
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                userDirectoryService() );
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
    public SearchService searchService() {
        return new SearchService( hazelcastInstance );
    }

    @Bean
    public EdmAuthorizationHelper edmAuthorizationHelper() {
        return new EdmAuthorizationHelper( dataModelService(), authorizationManager() );
    }

    @Bean
    public PermissionsRequestsQueryService permissionsRequestsQueryService() {
        return new PermissionsRequestsQueryService( session );
    }

    @Bean
    public PermissionsRequestsManager permissionsRequestsManager() {
        return new HazelcastPermissionsRequestsService(
                hazelcastInstance,
                permissionsRequestsQueryService(),
                authorizationManager() );
    }

}
