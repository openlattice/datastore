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

package com.dataloom.organizations.controllers;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.dataloom.authorization.AbstractSecurableObjectResolveTypeService;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.authorization.ForbiddenException;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.Principals;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.directory.pojo.Auth0UserBasic;
import com.dataloom.organization.Organization;
import com.dataloom.organization.OrganizationsApi;
import com.dataloom.organization.roles.OrganizationRole;
import com.dataloom.organization.roles.RoleKey;
import com.dataloom.organizations.HazelcastOrganizationService;
import com.dataloom.organizations.roles.RolesUtil;
import com.dataloom.streams.StreamUtil;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

@RestController
@RequestMapping( OrganizationsApi.CONTROLLER )
public class OrganizationsController implements AuthorizingComponent, OrganizationsApi {

    @Inject
    private AuthorizationManager                      authorizations;

    @Inject
    private HazelcastOrganizationService              organizations;

    @Inject
    private AbstractSecurableObjectResolveTypeService securableObjectTypes;

    @Override
    @GetMapping(
        value = { "", "/" },
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Organization> getOrganizations() {
        return getAccessibleObjects( SecurableObjectType.Organization, EnumSet.of( Permission.READ ) )
                .filter( Predicates.notNull()::apply ).map( AuthorizationUtils::getLastAclKeySafely )
                .map( organizations::getOrganization )::iterator;
    }

    @Override
    @PostMapping(
        value = { "", "/" },
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public UUID createOrganizationIfNotExists( @RequestBody Organization organization ) {
        organizations.createOrganization( Principals.getCurrentUser(), organization );
        securableObjectTypes.createSecurableObjectType( ImmutableList.of( organization.getId() ),
                SecurableObjectType.Organization );
        return organization.getId();
    }

    @Override
    @GetMapping(
        value = ID_PATH,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Organization getOrganization( @PathVariable( ID ) UUID organizationId ) {
        ensureRead( organizationId );
        Organization org = organizations.getOrganization( organizationId );
        Set<OrganizationRole> authorizedRoles = getAuthorizedRoles( organizationId, Permission.READ );
        return new Organization(
                Optional.of( org.getId() ),
                org.getTitle(),
                Optional.of( org.getDescription() ),
                org.getAutoApprovedEmails(),
                org.getMembers(),
                authorizedRoles );
    }

    @Override
    @DeleteMapping(
        value = ID_PATH,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void destroyOrganization( @PathVariable( ID ) UUID organizationId ) {
        List<UUID> aclKey = ensureOwner( organizationId );

        organizations.destroyOrganization( organizationId );
        authorizations.deletePermissions( aclKey );
        securableObjectTypes.deleteSecurableObjectType( ImmutableList.of( organizationId ) );
        return null;
    }

    @Override
    @PutMapping(
        value = ID_PATH + TITLE,
        consumes = MediaType.TEXT_PLAIN_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void updateTitle( @PathVariable( ID ) UUID organizationId, @RequestBody String title ) {
        ensureOwner( organizationId );
        organizations.updateTitle( organizationId, title );
        return null;
    }

    @Override
    @PutMapping(
        value = ID_PATH + DESCRIPTION,
        consumes = MediaType.TEXT_PLAIN_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void updateDescription( @PathVariable( ID ) UUID organizationId, @RequestBody String description ) {
        ensureOwner( organizationId );
        organizations.updateDescription( organizationId, description );
        return null;
    }

    @Override
    @GetMapping(
        value = ID_PATH + EMAIL_DOMAINS,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Set<String> getAutoApprovedEmailDomains( @PathVariable( ID ) UUID organizationId ) {
        ensureOwner( organizationId );
        return organizations.getAutoApprovedEmailDomains( organizationId );
    }

    @Override
    @PutMapping(
        value = ID_PATH + EMAIL_DOMAINS,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void setAutoApprovedEmailDomain(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody Set<String> emailDomains ) {
        ensureOwner( organizationId );
        organizations.setAutoApprovedEmailDomains( organizationId, emailDomains );
        return null;
    }

    @Override
    @PostMapping(
        value = ID_PATH + EMAIL_DOMAINS,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Void addAutoApprovedEmailDomains(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody Set<String> emailDomains ) {
        ensureOwner( organizationId );
        organizations.addAutoApprovedEmailDomains( organizationId, emailDomains );
        return null;
    }

    @Override
    @DeleteMapping(
        value = ID_PATH + EMAIL_DOMAINS,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void removeAutoApprovedEmailDomains(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody Set<String> emailDomains ) {
        ensureOwner( organizationId );
        organizations.removeAutoApprovedEmailDomains( organizationId, emailDomains );
        return null;
    }

    @Override
    @PutMapping(
        value = ID_PATH + EMAIL_DOMAINS + EMAIL_DOMAIN_PATH )
    public Void addAutoApprovedEmailDomain(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( EMAIL_DOMAIN ) String emailDomain ) {
        ensureOwner( organizationId );
        organizations.addAutoApprovedEmailDomains( organizationId, ImmutableSet.of( emailDomain ) );
        return null;
    }

    @Override
    @DeleteMapping(
        value = ID_PATH + EMAIL_DOMAINS + EMAIL_DOMAIN_PATH )
    public Void removeAutoApprovedEmailDomain(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( EMAIL_DOMAIN ) String emailDomain ) {
        ensureOwner( organizationId );
        organizations.removeAutoApprovedEmailDomains( organizationId, ImmutableSet.of( emailDomain ) );
        return null;
    }

    @Override
    @GetMapping(
        value = ID_PATH + PRINCIPALS )
    public Set<Principal> getPrincipals( @PathVariable( ID ) UUID organizationId ) {
        ensureRead( organizationId );
        Set<Principal> members = organizations.getMembers( organizationId );
        Set<Principal> roles = getAuthorizedPrincipals( organizationId, Permission.READ );
        return Sets.union( members, roles );
    }

    @Override
    @GetMapping(
        value = ID_PATH + PRINCIPALS + MEMBERS )
    public Set<Principal> getMembers( @PathVariable( ID ) UUID organizationId ) {
        ensureRead( organizationId );
        return organizations.getMembers( organizationId );
    }

    @Deprecated
    @Override
    @PutMapping(
        value = ID_PATH + PRINCIPALS + TYPE_PATH + PRINCIPAL_ID_PATH )
    public Void addPrincipal(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( TYPE ) PrincipalType principalType,
            @PathVariable( PRINCIPAL_ID ) String principalId ) {
        ensureOwner( organizationId );
        organizations.addPrincipal( Principals.getCurrentUser(),
                organizationId,
                new Principal( principalType, principalId ) );
        return null;
    }

    @Deprecated
    @Override
    @DeleteMapping(
        value = ID_PATH + PRINCIPALS + TYPE_PATH + PRINCIPAL_ID_PATH )
    public Void removePrincipal(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( TYPE ) PrincipalType principalType,
            @PathVariable( PRINCIPAL_ID ) String principalId ) {
        ensureOwner( organizationId );
        organizations.removePrincipal( organizationId, new Principal( principalType, principalId ) );
        return null;
    }

    @Override
    @PutMapping(
        value = ID_PATH + PRINCIPALS + MEMBERS + USER_ID_PATH )
    public Void addMember(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( USER_ID ) String userId ) {
        organizations.addMembers( organizationId, ImmutableSet.of( new Principal( PrincipalType.USER, userId ) ) );
        return null;
    }

    @Override
    @DeleteMapping(
        value = ID_PATH + PRINCIPALS + MEMBERS + USER_ID_PATH )
    public Void removeMember(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( USER_ID ) String userId ) {
        organizations.removeMembers( organizationId, ImmutableSet.of( new Principal( PrincipalType.USER, userId ) ) );
        return null;
    }

    @Override
    @PostMapping(
        value = ROLES,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public UUID createRole( @RequestBody OrganizationRole role ) {
        ensureOwner( role.getOrganizationId() );
        organizations.ensureValidOrganizationRole( role );

        organizations.createRoleIfNotExists( Principals.getCurrentUser(), role );
        return role.getId();
    }

    @Override
    @GetMapping(
        value = ID_PATH + PRINCIPALS + ROLES,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Set<OrganizationRole> getRoles( @PathVariable( ID ) UUID organizationId ) {
        ensureRead( organizationId );
        return getAuthorizedRoles( organizationId, Permission.READ );
    }

    private Set<Principal> getAuthorizedPrincipals( UUID organizationId, Permission permission ) {
        return StreamUtil.stream( organizations.getRolesInFull( organizationId ) )
                .filter( role -> isAuthorized( permission ).test( role.getAclKey() ) )
                .map( role -> RolesUtil.getPrincipal( role ) ).collect( Collectors.toSet() );
    }

    private Set<OrganizationRole> getAuthorizedRoles( UUID organizationId, Permission permission ) {
        return StreamUtil.stream( organizations.getRolesInFull( organizationId ) )
            .filter( role -> isAuthorized( permission ).test( role.getAclKey() ) )
            .collect( Collectors.toSet() );
    }

    @Override
    @GetMapping(
        value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public OrganizationRole getRole( @PathVariable( ID ) UUID organizationId, @PathVariable( ROLE_ID ) UUID roleId ) {
        List<UUID> aclKey = Arrays.asList( organizationId, roleId );
        if ( isAuthorized( Permission.READ ).test( aclKey ) ) {
            return organizations.getRoleInFull( new RoleKey( organizationId, roleId ) );
        } else {
            throw new ForbiddenException( "Unable to find role: " + aclKey );
        }
    }

    @Override
    @PutMapping(
        value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + TITLE,
        consumes = MediaType.TEXT_PLAIN_VALUE )
    public Void updateRoleTitle(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId,
            @RequestBody String title ) {
        ensureRoleAdminAccess( organizationId, roleId );
        organizations.updateRoleTitle( new RoleKey( organizationId, roleId ), title );
        return null;
    }

    @Override
    @PutMapping(
        value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + DESCRIPTION,
        consumes = MediaType.TEXT_PLAIN_VALUE )
    public Void updateRoleDescription(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId,
            @RequestBody String description ) {
        ensureRoleAdminAccess( organizationId, roleId );
        organizations.updateRoleDescription( new RoleKey( organizationId, roleId ), description );
        return null;
    }

    @Override
    @DeleteMapping(
        value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH )
    public Void deleteRole( @PathVariable( ID ) UUID organizationId, @PathVariable( ROLE_ID ) UUID roleId ) {
        ensureRoleAdminAccess( organizationId, roleId );
        organizations.deleteRole( new RoleKey( organizationId, roleId ) );
        return null;
    }

    @Override
    @GetMapping(
        value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Auth0UserBasic> getAllUsersOfRole(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId ) {
        return organizations.getAllUserProfilesOfRole( new RoleKey( organizationId, roleId ) )::iterator;
    }

    @Override
    @PutMapping(
        value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS + USER_ID_PATH )
    public Void addRoleToUser(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId,
            @PathVariable( USER_ID ) String userId ) {
        organizations.addRoleToUser( new RoleKey( organizationId, roleId ),
                new Principal( PrincipalType.USER, userId ) );
        return null;
    }

    @Override
    @DeleteMapping(
        value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS + USER_ID_PATH )
    public Void removeRoleFromUser(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId,
            @PathVariable( USER_ID ) String userId ) {
        organizations.removeRoleFromUser( new RoleKey( organizationId, roleId ),
                new Principal( PrincipalType.USER, userId ) );
        return null;
    }

    private void ensureRoleAdminAccess( UUID organizationId, UUID roleId ) {
        ensureOwner( organizationId );

        List<UUID> aclKey = ImmutableList.of( organizationId, roleId );
        accessCheck( aclKey, EnumSet.of( Permission.OWNER ) );
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

    private List<UUID> ensureOwner( UUID organizationId ) {
        List<UUID> aclKey = ImmutableList.of( organizationId );
        accessCheck( aclKey, EnumSet.of( Permission.OWNER ) );
        return aclKey;
    }

    private List<UUID> ensureRead( UUID organizationId ) {
        List<UUID> aclKey = ImmutableList.of( organizationId );
        accessCheck( aclKey, EnumSet.of( Permission.READ ) );
        return aclKey;
    }

}
