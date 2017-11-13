/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.datastore.pods;

import com.dataloom.authorization.*;
import com.dataloom.clustering.DistributedClusterer;
import com.dataloom.data.DataGraphManager;
import com.dataloom.data.DataGraphService;
import com.dataloom.data.DatasourceManager;
import com.dataloom.data.ids.HazelcastEntityKeyIdService;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.data.serializers.FullQualifedNameJacksonSerializer;
import com.dataloom.data.storage.CassandraEntityDatastore;
import com.dataloom.datastore.apps.services.AppService;
import com.dataloom.datastore.linking.services.SimpleElasticSearchBlocker;
import com.dataloom.datastore.linking.services.SimpleMatcher;
import com.dataloom.datastore.scripts.EmptyPermissionRemover;
import com.dataloom.datastore.services.AnalysisService;
import com.dataloom.datastore.services.LinkingService;
import com.dataloom.datastore.services.SearchService;
import com.dataloom.datastore.services.SyncTicketService;
import com.dataloom.directory.UserDirectoryService;
import com.dataloom.edm.properties.CassandraTypeManager;
import com.dataloom.edm.schemas.SchemaQueryService;
import com.dataloom.edm.schemas.cassandra.CassandraSchemaQueryService;
import com.dataloom.edm.schemas.manager.HazelcastSchemaManager;
import com.dataloom.graph.core.GraphQueryService;
import com.dataloom.graph.core.LoomGraph;
import com.dataloom.linking.CassandraLinkingGraphsQueryService;
import com.dataloom.linking.HazelcastLinkingGraphs;
import com.dataloom.linking.HazelcastListingService;
import com.dataloom.linking.HazelcastVertexMergingService;
import com.dataloom.linking.components.Blocker;
import com.dataloom.linking.components.Clusterer;
import com.dataloom.linking.components.Matcher;
import com.dataloom.mappers.ObjectMappers;
import com.dataloom.matching.DistributedMatcher;
import com.dataloom.merging.DistributedMerger;
import com.dataloom.neuron.Neuron;
import com.dataloom.neuron.pods.NeuronPod;
import com.dataloom.organizations.HazelcastOrganizationService;
import com.dataloom.organizations.roles.HazelcastPrincipalService;
import com.dataloom.organizations.roles.RolesManager;
import com.dataloom.organizations.roles.RolesQueryService;
import com.dataloom.organizations.roles.TokenExpirationTracker;
import com.dataloom.requests.HazelcastRequestsManager;
import com.dataloom.requests.RequestQueryService;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.datastore.services.CassandraEntitySetManager;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.EdmService;
import com.kryptnostic.datastore.services.ODataStorageService;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.pods.CassandraPod;
import digital.loom.rhizome.authentication.Auth0Pod;
import digital.loom.rhizome.configuration.auth0.Auth0Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

@Configuration
@Import( {
        Auth0Pod.class,
        CassandraPod.class,
        NeuronPod.class
} )
public class DatastoreServicesPod {

    @Inject
    private CassandraConfiguration cassandraConfiguration;

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Inject
    private Session session;

    @Inject
    private Auth0Configuration auth0Configuration;

    @Inject
    private ListeningExecutorService executor;

    @Inject
    private EventBus eventBus;

    @Inject
    private Neuron neuron;

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
        return new HazelcastAuthorizationService( hazelcastInstance, authorizationQueryService(), eventBus );
    }

    @Bean
    public AbstractSecurableObjectResolveTypeService securableObjectTypes() {
        return new HazelcastAbstractSecurableObjectResolveTypeService( hazelcastInstance );
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
                schemaManager(),
                datasourceManager() );
    }

    @Bean
    public HazelcastAclKeyReservationService aclKeyReservationService() {
        return new HazelcastAclKeyReservationService( hazelcastInstance );
    }

    @Bean
    public HazelcastListingService hazelcastListingService() {
        return new HazelcastListingService( hazelcastInstance );
    }

    @Bean
    public HazelcastLinkingGraphs linkingGraph() {
        return new HazelcastLinkingGraphs( hazelcastInstance );
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
    public CassandraEntityDatastore cassandraDataManager() {
        return new CassandraEntityDatastore(
                session,
                hazelcastInstance,
                executor,
                defaultObjectMapper(),
                idService(),
                linkingGraph(),
                loomGraph(),
                datasourceManager() );
    }

    @Bean
    public RolesQueryService rolesQueryService() {
        return new RolesQueryService( session );
    }

    @Bean
    public TokenExpirationTracker tokenTracker() {
        return new TokenExpirationTracker( hazelcastInstance );
    }

    @PostConstruct
    public void setExpiringTokenTracker() {
        Principals.setExpiringTokenTracker( tokenTracker() );
    }

    @Bean
    public RolesManager rolesService() {
        return new HazelcastPrincipalService(
                hazelcastInstance,
                aclKeyReservationService(),
                userDirectoryService(),
                authorizationManager() );
    }

    @Bean
    public HazelcastOrganizationService organizationsManager() {
        return new HazelcastOrganizationService(
                cassandraConfiguration.getKeyspace(),
                session,
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                userDirectoryService(),
                rolesService() );
    }

    @Bean
    public DatasourceManager datasourceManager() {
        return new DatasourceManager( session, hazelcastInstance );
    }

    @Bean
    public UserDirectoryService userDirectoryService() {
        return new UserDirectoryService( auth0Configuration.getToken() );
    }

    @Bean
    public SearchService searchService() {
        return new SearchService();
    }

    @Bean
    public EdmAuthorizationHelper edmAuthorizationHelper() {
        return new EdmAuthorizationHelper( dataModelService(), authorizationManager() );
    }

    @Bean
    public SyncTicketService sts() {
        return new SyncTicketService( hazelcastInstance );
    }

    @Bean
    public RequestQueryService rqs() {
        return new RequestQueryService( cassandraConfiguration.getKeyspace(), session );
    }

    @Bean
    public HazelcastRequestsManager hazelcastRequestsManager() {
        return new HazelcastRequestsManager( hazelcastInstance, rqs(), neuron );
    }

    @Bean
    public Blocker simpleElasticSearchBlocker() {
        return new SimpleElasticSearchBlocker( dataModelService(), cassandraDataManager(), searchService() );
    }

    @Bean
    public Matcher simpleMatcher() {
        return new SimpleMatcher( dataModelService() );
    }

    @Bean
    public CassandraLinkingGraphsQueryService cgqs() {
        return new CassandraLinkingGraphsQueryService( cassandraConfiguration.getKeyspace(), session );
    }

    @Bean
    public Clusterer clusterer() {
        return new DistributedClusterer( hazelcastInstance );
    }

    @Bean
    public DistributedMatcher matcher() {
        return new DistributedMatcher( hazelcastInstance, dataModelService() );
    }

    @Bean
    public DistributedMerger merger() {
        return new DistributedMerger( hazelcastInstance,
                hazelcastListingService(),
                dataModelService(),
                datasourceManager() );
    }

    @Bean
    public LinkingService linkingService() {
        return new LinkingService(
                cassandraConfiguration.getKeyspace(),
                session,
                linkingGraph(),
                simpleElasticSearchBlocker(),
                matcher(),
                clusterer(),
                merger(),
                hazelcastInstance,
                eventBus,
                hazelcastListingService(),
                dataModelService(),
                dataGraphService(),
                datasourceManager(),
                cassandraDataManager(),
                loomGraph(),
                idService(),
                vms(),
                cgqs(),
                defaultObjectMapper() );
    }

    @Bean
    public AnalysisService analysisService() {
        return new AnalysisService();
    }

    @Bean
    public GraphQueryService graphQueryService() {
        return new GraphQueryService( session );
    }

    @Bean
    public LoomGraph loomGraph() {
        return new LoomGraph( executor, hazelcastInstance );
    }

    @Bean
    public HazelcastEntityKeyIdService idService() {
        return new HazelcastEntityKeyIdService( hazelcastInstance, executor );
    }

    @Bean
    public DataGraphManager dataGraphService() {
        return new DataGraphService(
                hazelcastInstance,
                cassandraDataManager(),
                loomGraph(),
                idService(),
                executor,
                eventBus );
    }

    @Bean
    public HazelcastVertexMergingService vms() {
        return new HazelcastVertexMergingService( hazelcastInstance );
    }

    @Bean
    public AppService appService() {
        return new AppService( hazelcastInstance,
                dataModelService(),
                organizationsManager(),
                authorizationQueryService(),
                authorizationManager(),
                rolesService(),
                aclKeyReservationService() );
    }

    // Startup scripts
    @PostConstruct
    public void scripts() {
        // Populate entity set contacts
        // new EntitySetContactsPopulator(
        // cassandraConfiguration.getKeyspace(),
        // session,
        // dataModelService(),
        // userDirectoryService(),
        // hazelcastInstance ).run();

        // Remove empty permissions
        new EmptyPermissionRemover( cassandraConfiguration.getKeyspace(), session ).run();

        // Create default organization and roles
        // new DefaultOrganizationCreator( organizationsManager(), rolesService() ).run();
    }
}
