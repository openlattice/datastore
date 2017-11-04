package com.dataloom.datastore.apps.controllers;

import com.dataloom.apps.App;
import com.dataloom.apps.AppApi;
import com.dataloom.apps.AppConfig;
import com.dataloom.apps.AppType;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principals;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.datastore.apps.services.AppService;
import com.dataloom.organization.Organization;
import com.dataloom.organizations.HazelcastOrganizationService;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
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
            path = TYPE_PATH + ID_PATH,
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public AppType getAppType( @PathVariable( ID ) UUID id ) {
        return appService.getAppType( id );
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
        ensureOwnerAccess( ImmutableList.of( organizationId ) );
        appService.installApp( appId, organizationId, prefix, Principals.getCurrentUser() );
    }

    private Iterable<Organization> getAvailableOrgs() {
        return getAccessibleObjects( SecurableObjectType.Organization,
                EnumSet.of( Permission.READ ) )
                .filter( Predicates.notNull()::apply ).map( AuthorizationUtils::getLastAclKeySafely )
                .map( organizations::getOrganization )::iterator;
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

    @Override public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }
}
