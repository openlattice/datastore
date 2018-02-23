package com.dataloom.datastore.apps.controllers;

import com.openlattice.apps.App;
import com.openlattice.apps.AppApi;
import com.openlattice.apps.AppConfig;
import com.openlattice.apps.AppType;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.openlattice.authorization.Permission;
import com.dataloom.authorization.Principals;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.datastore.apps.services.AppService;
import com.openlattice.edm.requests.MetadataUpdate;
import com.openlattice.organization.Organization;
import com.dataloom.organizations.HazelcastOrganizationService;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.openlattice.authorization.AclKey;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import java.util.*;

@RestController
@RequestMapping( AppApi.CONTROLLER )
public class AppController implements AppApi, AuthorizingComponent {

    @Inject
    private AuthorizationManager authorizations;

    @Inject
    private AppService appService;

    @Inject
    private HazelcastOrganizationService organizations;

    @Override
    @RequestMapping(
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<App> getApps() {
        return appService.getApps();
    }

    @Override
    @RequestMapping(
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public UUID createApp( @RequestBody App app ) {
        ensureAdminAccess();
        return appService.createApp( app );
    }

    @Override
    @RequestMapping(
            path = TYPE_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public UUID createAppType( @RequestBody AppType appType ) {
        ensureAdminAccess();
        return appService.createAppType( appType );
    }

    @Override
    @RequestMapping(
            path = ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public App getApp( @PathVariable( ID ) UUID id ) {
        return appService.getApp( id );
    }

    @Override
    @RequestMapping(
            path = LOOKUP_PATH + NAME_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public App getApp( @PathVariable( NAME ) String name ) {
        return appService.getApp( name );
    }

    @Override
    @RequestMapping(
            path = TYPE_PATH + ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public AppType getAppType( @PathVariable( ID ) UUID id ) {
        return appService.getAppType( id );
    }

    @Override
    @RequestMapping(
            path = TYPE_PATH + LOOKUP_PATH + NAMESPACE_PATH + NAME_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public AppType getAppType( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        return appService.getAppType( new FullQualifiedName( namespace, name ) );
    }

    @Override
    @RequestMapping(
            path = TYPE_PATH + BULK_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<UUID, AppType> getAppTypes( @RequestBody Set<UUID> appTypeIds ) {
        return appService.getAppTypes( appTypeIds );
    }

    @Override
    @RequestMapping(
            path = ID_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public void deleteApp( @PathVariable( ID ) UUID id ) {
        ensureAdminAccess();
        appService.deleteApp( id );
    }

    @Override
    @RequestMapping(
            path = TYPE_PATH + ID_PATH,
            method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public void deleteAppType( @PathVariable( ID ) UUID id ) {
        ensureAdminAccess();
        appService.deleteAppType( id );
    }

    @Override
    @RequestMapping(
            path = INSTALL_PATH + ID_PATH + ORGANIZATION_ID_PATH + PREFIX_PATH,
            method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public void installApp(
            @PathVariable( ID ) UUID appId,
            @PathVariable( ORGANIZATION_ID ) UUID organizationId,
            @PathVariable( PREFIX ) String prefix ) {
        ensureOwnerAccess( new AclKey( organizationId ) );
        appService.installApp( appId, organizationId, prefix, Principals.getCurrentUser() );
    }

    private Iterable<Organization> getAvailableOrgs() {
        return getAccessibleObjects( SecurableObjectType.Organization,
                EnumSet.of( Permission.READ ) )
                .filter( Predicates.notNull()::apply ).map( AuthorizationUtils::getLastAclKeySafely )
                .map( organizations::getOrganization )
                .filter( organization -> organization!=null )::iterator;
    }

    @Override
    @RequestMapping(
            path = CONFIG_PATH + ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public List<AppConfig> getAvailableAppConfigs( @PathVariable( ID ) UUID appId ) {
        Iterable<Organization> orgs = getAvailableOrgs();
        return appService.getAvailableConfigs( appId, Principals.getCurrentPrincipals(), orgs );
    }

    @Override
    @RequestMapping(
            path = UPDATE_PATH + ID_PATH + APP_TYPE_ID_PATH,
            method = RequestMethod.GET )
    public void addAppTypeToApp( @PathVariable( ID ) UUID appId, @PathVariable( APP_TYPE_ID ) UUID appTypeId ) {
        ensureAdminAccess();
        appService.addAppTypesToApp( appId, ImmutableSet.of( appTypeId ) );
    }

    @Override
    @RequestMapping(
            path = UPDATE_PATH + ID_PATH + APP_TYPE_ID_PATH,
            method = RequestMethod.DELETE )
    public void removeAppTypeFromApp( @PathVariable( ID ) UUID appId, @PathVariable( APP_TYPE_ID ) UUID appTypeId ) {
        ensureAdminAccess();
        appService.removeAppTypesFromApp( appId, ImmutableSet.of( appTypeId ) );
    }

    @Override
    @RequestMapping(
            path = UPDATE_PATH + ID_PATH + APP_ID_PATH + APP_TYPE_ID_PATH + ENTITY_SET_ID_PATH,
            method = RequestMethod.GET )
    public void updateAppEntitySetConfig(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( APP_ID ) UUID appId,
            @PathVariable( APP_TYPE_ID ) UUID appTypeId,
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId ) {
        ensureOwnerAccess( new AclKey( organizationId ) );
        appService.updateAppConfigEntitySetId( organizationId, appId, appTypeId, entitySetId );
    }

    @Override
    @RequestMapping(
            path = UPDATE_PATH + ID_PATH + APP_ID_PATH + APP_TYPE_ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void updateAppEntitySetPermissionsConfig(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( APP_ID ) UUID appId,
            @PathVariable( APP_TYPE_ID ) UUID appTypeId,
            @RequestBody Set<Permission> permissions ) {
        ensureOwnerAccess( new AclKey( organizationId ) );
        appService.updateAppConfigPermissions( organizationId, appId, appTypeId, EnumSet.copyOf( permissions ) );
    }

    @Override
    @RequestMapping(
            path = UPDATE_PATH + ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void updateAppMetadata(
            @PathVariable( ID ) UUID appId,
            @RequestBody MetadataUpdate metadataUpdate ) {
        ensureAdminAccess();
        appService.updateAppMetadata( appId, metadataUpdate );
    }

    @Override
    @RequestMapping(
            path =  TYPE_PATH + UPDATE_PATH + ID_PATH,
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public void updateAppTypeMetadata(
            @PathVariable( ID ) UUID appTypeId,
            @RequestBody MetadataUpdate metadataUpdate ) {
        ensureAdminAccess();
        appService.updateAppTypeMetadata( appTypeId, metadataUpdate );
    }

    @Override public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

}
