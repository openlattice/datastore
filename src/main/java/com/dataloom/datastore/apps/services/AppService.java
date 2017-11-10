package com.dataloom.datastore.apps.services;

import com.dataloom.apps.App;
import com.dataloom.apps.AppConfig;
import com.dataloom.apps.AppConfigKey;
import com.dataloom.apps.AppType;
import com.dataloom.authorization.*;
import com.dataloom.edm.EntitySet;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organization.Organization;
import com.dataloom.organization.roles.Role;
import com.dataloom.organizations.HazelcastOrganizationService;
import com.dataloom.organizations.roles.RolesManager;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.exceptions.BadRequestException;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.util.Util;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.*;
import java.util.stream.Collectors;

public class AppService {
    private final IMap<UUID, App>          apps;
    private final IMap<UUID, AppType>      appTypes;
    private final IMap<AppConfigKey, UUID> appConfigs;
    private final IMap<String, UUID>       aclKeys;

    private final EdmManager                        edmService;
    private final HazelcastOrganizationService      organizationService;
    private final AuthorizationQueryService         authorizations;
    private final AuthorizationManager              authorizationService;
    private final RolesManager                      rolesService;
    private final HazelcastAclKeyReservationService reservations;

    public AppService(
            HazelcastInstance hazelcast,
            EdmManager edmService,
            HazelcastOrganizationService organizationService,
            AuthorizationQueryService authorizations,
            AuthorizationManager authorizationService,
            RolesManager rolesService,
            HazelcastAclKeyReservationService reservations
    ) {
        this.apps = hazelcast.getMap( HazelcastMap.APPS.name() );
        this.appTypes = hazelcast.getMap( HazelcastMap.APP_TYPES.name() );
        this.appConfigs = hazelcast.getMap( HazelcastMap.APP_CONFIGS.name() );
        this.aclKeys = hazelcast.getMap( HazelcastMap.ACL_KEYS.name() );
        this.edmService = edmService;
        this.organizationService = organizationService;
        this.authorizations = authorizations;
        this.authorizationService = authorizationService;
        this.rolesService = rolesService;
        this.reservations = reservations;
    }

    public Iterable<App> getApps() {
        return apps.values();
    }

    public UUID createApp( App app ) {
        reservations.reserveIdAndValidateType( app, app::getName );
        apps.putIfAbsent( app.getId(), app );
        return app.getId();
    }

    public void deleteApp( UUID appId ) {
        apps.delete( appId );
        reservations.release( appId );
    }

    public App getApp( UUID appId ) {
        return apps.get( appId );
    }

    public App getApp( String name ) {
        UUID id = Util.getSafely( aclKeys, name );
        return getApp( id );
    }

    private UUID generateEntitySet( UUID appTypeId, String prefix, Principal principal ) {
        AppType appType = getAppType( appTypeId );
        String name = prefix.concat( "_" ).concat( appType.getType().getNamespace() ).concat( "_" )
                .concat( appType.getType().getName() );
        String title = prefix.concat( " " ).concat( appType.getTitle() );
        Optional<String> description = Optional.of( prefix.concat( " " ).concat( appType.getDescription() ) );
        EntitySet entitySet = new EntitySet( Optional.absent(),
                appType.getEntityTypeId(),
                name,
                title,
                description,
                ImmutableSet.of() );
        edmService.createEntitySet( principal, entitySet );
        return edmService.getEntitySet( name ).getId();
    }

    private Map<Permission, UUID> createRolesForAppPermission(
            App app,
            UUID organizationId,
            EnumSet<Permission> permissions,
            Principal user ) {
        Map<Permission, UUID> result = Maps.newHashMap();
        permissions.forEach( permission -> {
            String title = app.getTitle().concat( " - " ).concat( permission.name() );
            String description = permission.name().concat( " permission for the " ).concat( app.getTitle() )
                    .concat( " app" );
            Role role = new Role( Optional.absent(),
                    organizationId,
                    title,
                    Optional.of( description ) );
            try {
                rolesService.createRoleIfNotExists( user, role );
                result.put( permission, role.getId() );
            } catch ( Exception e ) {
                throw new BadRequestException( "The requested app has already been installed for this organization" );
            }
        } );
        return result;
    }

    public void installApp( UUID appId, UUID organizationId, String prefix, Principal principal ) {
        App app = getApp( appId );
        Preconditions.checkNotNull( app, "The requested app with id %s does not exist.", appId.toString() );

        Set<Principal> organizationOwners = Sets
                .newHashSet( authorizations.getOwnersForSecurableObject( ImmutableList.of( organizationId ) ) );

        Map<Permission, UUID> appRoles = createRolesForAppPermission( app,
                organizationId,
                EnumSet.of( Permission.READ, Permission.WRITE, Permission.OWNER ), principal );

        app.getAppTypeIds().stream().forEach( appTypeId -> {
            UUID entitySetId = generateEntitySet( appTypeId, prefix, principal );
            appConfigs.put( new AppConfigKey( appId, organizationId, appTypeId ), entitySetId );
            organizationOwners.forEach( owner -> {
                authorizationService.addPermission( ImmutableList.of( entitySetId ),
                        owner,
                        EnumSet.of( Permission.DISCOVER,
                                Permission.LINK,
                                Permission.READ,
                                Permission.WRITE,
                                Permission.OWNER ) );
            } );
            appRoles.entrySet().forEach( entry -> {
                Permission permission = entry.getKey();
                // TODO: check if principal id should be role uuid or orgId|roleName
                Principal rolePrincipal = new Principal( PrincipalType.ROLE, entry.getValue().toString() );
                authorizationService
                        .addPermission( ImmutableList.of( entitySetId ), rolePrincipal, EnumSet.of( permission ) );
                edmService.getEntityType( appTypes.get( appTypeId ).getEntityTypeId() ).getProperties()
                        .forEach( propertyTypeId -> {
                            authorizationService.addPermission( ImmutableList.of( entitySetId, propertyTypeId ),
                                    rolePrincipal,
                                    EnumSet.of( permission ) );
                        } );
            } );
        } );
    }

    public UUID createAppType( AppType appType ) {
        reservations.reserveIdAndValidateType( appType );
        appTypes.putIfAbsent( appType.getId(), appType );
        return appType.getId();
    }

    public void deleteAppType( UUID id ) {
        appTypes.delete( id );
        reservations.release( id );
    }

    public AppType getAppType( UUID id ) {
        return appTypes.get( id );
    }

    public AppType getAppType( FullQualifiedName fqn ) {
        UUID id = Util.getSafely( aclKeys, Util.fqnToString( fqn ) );
        return getAppType( id );
    }

    public Map<UUID, AppType> getAppTypes( Set<UUID> appTypeIds ) {
        return appTypes.getAll( appTypeIds );
    }

    public List<AppConfig> getAvailableConfigs(
            UUID appId,
            Set<Principal> principals,
            Iterable<Organization> organizations ) {
        List<AppConfig> availableConfigs = Lists.newArrayList();

        App app = apps.get( appId );

        organizations.forEach( organization -> {
            try {
                Map<AppConfigKey, UUID> configs = appConfigs.getAll( app.getAppTypeIds().stream()
                        .map( id -> new AppConfigKey( appId, organization.getId(), id ) ).collect(
                                Collectors.toSet() ) );
                boolean authorized = true;
                for ( UUID entitySetId : configs.values() ) {

                    Set<Permission> permissions = authorizationService
                            .getSecurableObjectPermissions( ImmutableList.of( entitySetId ), principals );
                    if ( !permissions.contains( Permission.READ ) && !permissions.contains( Permission.WRITE ) )
                        authorized = false;
                }
                if ( authorized ) {
                    Map<String, UUID> config = configs.entrySet().stream().collect( Collectors
                            .toMap( entry -> appTypes.get( entry.getKey().getAppTypeId() ).getType()
                                    .getFullQualifiedNameAsString(), entry -> entry.getValue() ) );
                    availableConfigs.add( new AppConfig( organization, config ) );
                }
            } catch ( NullPointerException e ) {}
        } );
        return availableConfigs;
    }

}
