package com.dataloom.datastore.apps.services;

import com.dataloom.apps.*;
import com.dataloom.apps.processors.*;
import com.dataloom.authorization.*;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.organization.Organization;
import com.dataloom.organization.roles.Role;
import com.dataloom.organizations.HazelcastOrganizationService;
import com.dataloom.organizations.roles.SecurePrincipalsManager;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.exceptions.BadRequestException;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.util.Util;
import com.openlattice.authorization.AclKey;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.*;
import java.util.stream.Collectors;

public class AppService {
    private final IMap<UUID, App>                    apps;
    private final IMap<UUID, AppType>                appTypes;
    private final IMap<AppConfigKey, AppTypeSetting> appConfigs;
    private final IMap<String, UUID>                 aclKeys;

    private final EdmManager                        edmService;
    private final HazelcastOrganizationService      organizationService;
    private final AuthorizationQueryService         authorizations;
    private final AuthorizationManager              authorizationService;
    private final SecurePrincipalsManager           principalsService;
    private final HazelcastAclKeyReservationService reservations;

    public AppService(
            HazelcastInstance hazelcast,
            EdmManager edmService,
            HazelcastOrganizationService organizationService,
            AuthorizationQueryService authorizations,
            AuthorizationManager authorizationService,
            SecurePrincipalsManager principalsService,
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
        this.principalsService = principalsService;
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

    private Map<Permission, Principal> createRolesForAppPermission(
            App app,
            UUID organizationId,
            EnumSet<Permission> permissions,
            Principal user ) {
        Map<Permission, Principal> result = Maps.newHashMap();
        permissions.forEach( permission -> {
            String title = app.getTitle().concat( " - " ).concat( permission.name() );
            Principal principal = new Principal( PrincipalType.ROLE,
                    organizationId.toString().concat( "|" ).concat( title ) );
            String description = permission.name().concat( " permission for the " ).concat( app.getTitle() )
                    .concat( " app" );
            Role role = new Role( Optional.absent(),
                    organizationId,
                    principal,
                    title,
                    Optional.of( description ) );
            try {
                principalsService.createSecurablePrincipalIfNotExists( user, role );
                result.put( permission, principal );
            } catch ( Exception e ) {
                throw new BadRequestException( "The requested app has already been installed for this organization" );
            }
        } );
        return result;
    }

    public void installApp( UUID appId, UUID organizationId, String prefix, Principal principal ) {
        App app = getApp( appId );
        Preconditions.checkNotNull( app, "The requested app with id %s does not exist.", appId.toString() );

        EnumSet<Permission> defaultPermissions = EnumSet
                .of( Permission.DISCOVER, Permission.LINK, Permission.READ, Permission.WRITE, Permission.OWNER );

        Map<Permission, Principal> appRoles = createRolesForAppPermission( app,
                organizationId,
                EnumSet.of( Permission.READ, Permission.WRITE, Permission.OWNER ),
                principal );

        Principal appPrincipal = new Principal( PrincipalType.APP,
                AppConfig.getAppPrincipalId( appId, organizationId ) );

        app.getAppTypeIds().stream().forEach( appTypeId -> {
            UUID entitySetId = generateEntitySet( appTypeId, prefix, principal );
            appConfigs.put( new AppConfigKey( appId, organizationId, appTypeId ),
                    new AppTypeSetting( entitySetId, EnumSet.of( Permission.READ, Permission.WRITE ) ) );
            authorizationService.addPermission( new AclKey( entitySetId ), appPrincipal, defaultPermissions );

            appRoles.entrySet().forEach( entry -> {
                Permission permission = entry.getKey();
                Principal rolePrincipal = entry.getValue();
                authorizationService
                        .addPermission( new AclKey( entitySetId ), rolePrincipal, EnumSet.of( permission ) );
                edmService.getEntityType( appTypes.get( appTypeId ).getEntityTypeId() ).getProperties()
                        .forEach( propertyTypeId -> {
                            AclKey aclKeys = new AclKey( entitySetId, propertyTypeId );
                            authorizationService.addPermission( aclKeys, appPrincipal, defaultPermissions );
                            authorizationService.addPermission( aclKeys, rolePrincipal, EnumSet.of( permission ) );
                        } );
            } );
        } );
        organizationService.addAppToOrg( organizationId, appId );
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

    private boolean authorized(
            Collection<AppTypeSetting> requiredSettings,
            Principal appPrincipal,
            Set<Principal> userPrincipals ) {
        boolean authorized = true;
        for ( AppTypeSetting setting : requiredSettings ) {
            AclKey aclKey = new AclKey( setting.getEntitySetId() );
            boolean appIsAuthorized = authorizationService
                    .checkIfHasPermissions( aclKey, ImmutableSet.of( appPrincipal ), setting.getPermissions() );
            boolean userIsAuthorized = authorizationService
                    .checkIfHasPermissions( aclKey, userPrincipals, setting.getPermissions() );
            if ( !appIsAuthorized || !userIsAuthorized )
                authorized = false;
        }
        return authorized;
    }

    public List<AppConfig> getAvailableConfigs(
            UUID appId,
            Set<Principal> principals,
            Iterable<Organization> organizations ) {

        List<AppConfig> availableConfigs = Lists.newArrayList();
        App app = apps.get( appId );

        organizations.forEach( organization -> {
            if ( organizationService.getOrganizationApps( organization.getId() ).contains( appId ) ) {
                Principal appPrincipal = new Principal( PrincipalType.APP,
                        AppConfig.getAppPrincipalId( app.getId(), organization.getId() ) );
                try {
                    Map<AppConfigKey, AppTypeSetting> configs = appConfigs.getAll( app.getAppTypeIds().stream()
                            .map( id -> new AppConfigKey( appId, organization.getId(), id ) ).collect(
                                    Collectors.toSet() ) );

                    if ( authorized( configs.values(), appPrincipal, principals ) ) {

                        Map<String, AppTypeSetting> config = configs.entrySet().stream().collect( Collectors
                                .toMap( entry -> appTypes.get( entry.getKey().getAppTypeId() ).getType()
                                        .getFullQualifiedNameAsString(), entry -> entry.getValue() ) );
                        AppConfig appConfig = new AppConfig( Optional.of( app.getId() ),
                                appPrincipal,
                                app.getTitle(),
                                Optional.of( app.getDescription() ),
                                app.getId(),
                                organization,
                                config );
                        availableConfigs.add( appConfig );
                    }
                } catch ( NullPointerException e ) {}
            }
        } );
        return availableConfigs;
    }

    public void addAppTypesToApp( UUID appId, Set<UUID> appTypeIds ) {
        apps.executeOnKey( appId, new AddAppTypesToAppProcessor( appTypeIds ) );
    }

    public void removeAppTypesFromApp( UUID appId, Set<UUID> appTypeIds ) {
        apps.executeOnKey( appId, new RemoveAppTypesFromAppProcessor( appTypeIds ) );
    }

    public void updateAppConfigEntitySetId( UUID organizationId, UUID appId, UUID appTypeId, UUID entitySetId ) {
        AppConfigKey key = new AppConfigKey( organizationId, appId, appTypeId );
        appConfigs.executeOnKey( key, new UpdateAppConfigEntitySetProcessor( entitySetId ) );
    }

    public void updateAppConfigPermissions(
            UUID organizationId,
            UUID appId,
            UUID appTypeId,
            EnumSet<Permission> permissions ) {
        AppConfigKey key = new AppConfigKey( organizationId, appId, appTypeId );
        appConfigs.executeOnKey( key, new UpdateAppConfigPermissionsProcessor( permissions ) );
    }

    public void updateAppMetadata( UUID appId, MetadataUpdate metadataUpdate ) {
        apps.executeOnKey( appId, new UpdateAppMetadataProcessor( metadataUpdate ) );
    }

    public void updateAppTypeMetadata( UUID appTypeId, MetadataUpdate metadataUpdate ) {
        appTypes.executeOnKey( appTypeId, new UpdateAppTypeMetadataProcessor( metadataUpdate ) );
    }

}
