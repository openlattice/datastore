/*
 * Copyright (C) 2018. OpenLattice, Inc.
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
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.datastore.organization.controllers;

import com.auth0.json.mgmt.users.User;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.openlattice.assembler.Assembler;
import com.openlattice.authorization.AccessCheck;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.Authorization;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.EdmAuthorizationHelper;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.authorization.Principals;
import com.openlattice.authorization.SecurableObjectResolveTypeService;
import com.openlattice.authorization.SecurablePrincipal;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.authorization.util.AuthorizationUtils;
import com.openlattice.controllers.exceptions.ForbiddenException;
import com.openlattice.controllers.exceptions.ResourceNotFoundException;
import com.openlattice.datastore.services.EntitySetManager;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.organization.OrganizationEntitySetFlag;
import com.openlattice.organization.OrganizationIntegrationAccount;
import com.openlattice.organization.OrganizationMember;
import com.openlattice.organization.OrganizationPrincipal;
import com.openlattice.organization.OrganizationsApi;
import com.openlattice.organization.roles.Role;
import com.openlattice.organizations.ExternalDatabaseManagementService;
import com.openlattice.organizations.Grant;
import com.openlattice.organizations.HazelcastOrganizationService;
import com.openlattice.organizations.Organization;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import org.apache.commons.lang3.NotImplementedException;
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

import javax.inject.Inject;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;

@RestController
@RequestMapping( OrganizationsApi.CONTROLLER )
public class OrganizationsController implements AuthorizingComponent, OrganizationsApi {

    @Inject
    private AuthorizationManager authorizations;

    @Inject
    private HazelcastOrganizationService organizations;

    @Inject
    private Assembler assembler;

    @Inject
    private SecurableObjectResolveTypeService securableObjectTypes;

    @Inject
    private SecurePrincipalsManager principalService;

    @Inject
    private EntitySetManager entitySetManager;

    @Inject
    private EdmAuthorizationHelper authzHelper;

    @Inject
    private ExternalDatabaseManagementService edms;

    @Timed
    @Override
    @GetMapping(
            value = { "", "/" },
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Organization> getOrganizations() {

        Set<AclKey> authorizedRoles = getAccessibleObjects( SecurableObjectType.Role, EnumSet.of( Permission.READ ) )
                .filter( Objects::nonNull ).collect( Collectors.toSet() );

        Iterable<Organization> orgs = organizations.getOrganizations(
                getAccessibleObjects( SecurableObjectType.Organization, EnumSet.of( Permission.READ ) )
                        .parallel()
                        .filter( Objects::nonNull )
                        .map( AuthorizationUtils::getLastAclKeySafely )
        );

        return StreamSupport.stream( orgs.spliterator(), false ).map( org -> {
            org.getRoles().removeIf( role -> !authorizedRoles.contains( role.getAclKey() ) );
            return org;
        } ).collect( Collectors.toList() );
    }

    @Timed
    @Override
    @PostMapping(
            value = { "", "/" },
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public UUID createOrganizationIfNotExists( @RequestBody Organization organization ) {
        checkArgument( organization.getConnections().isEmpty() || isAdmin(),
                "Must be admin to specify auto-enrollments" );
        organizations.createOrganization( Principals.getCurrentUser(), organization );
        securableObjectTypes.createSecurableObjectType( new AclKey( organization.getId() ),
                SecurableObjectType.Organization );
        return organization.getId();
    }

    @Timed
    @Override
    @GetMapping(
            value = ID_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Organization getOrganization( @PathVariable( ID ) UUID organizationId ) {
        ensureRead( organizationId );
        //TODO: Re-visit roles within an organization being defined as roles which have read on that organization.
        Organization org = organizations.getOrganization( organizationId );
        if ( org != null ) {
            Set<AclKey> authorizedRoleAclKeys = getAuthorizedRoleAclKeys( org.getRoles() );
            org.getRoles().removeIf( role -> !authorizedRoleAclKeys.contains( role.getAclKey() ) );
        }
        return org;
    }

    @Timed
    @Override
    @DeleteMapping( value = ID_PATH, produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void destroyOrganization( @PathVariable( ID ) UUID organizationId ) {
        AclKey aclKey = ensureOwner( organizationId );

        ensureObjectCanBeDeleted( organizationId );

        organizations.destroyOrganization( organizationId );
        edms.deleteOrganizationExternalDatabase( organizationId );
        authorizations.deletePermissions( aclKey );
        securableObjectTypes.deleteSecurableObjectType( new AclKey( organizationId ) );
        return null;
    }

    @Timed
    @Override
    @GetMapping( value = ID_PATH + INTEGRATION, produces = MediaType.APPLICATION_JSON_VALUE )
    public OrganizationIntegrationAccount getOrganizationIntegrationAccount( @PathVariable( ID ) UUID organizationId ) {
        ensureOwner( organizationId );
        return assembler.getOrganizationIntegrationAccount( organizationId );
    }

    @Timed
    @Override
    @GetMapping( value = ID_PATH + ENTITY_SETS, produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<UUID, Set<OrganizationEntitySetFlag>> getOrganizationEntitySets(
            @PathVariable( ID ) UUID organizationId ) {
        ensureRead( organizationId );
        return getOrganizationEntitySets( organizationId, EnumSet.allOf( OrganizationEntitySetFlag.class ) );
    }

    @Timed
    @Override
    @PostMapping( value = ID_PATH + ENTITY_SETS, produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<UUID, Set<OrganizationEntitySetFlag>> getOrganizationEntitySets(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody EnumSet<OrganizationEntitySetFlag> flagFilter ) {
        ensureRead( organizationId );
        final var orgPrincipal = organizations.getOrganizationPrincipal( organizationId );
        if( orgPrincipal == null ) {
            return null;
        }
        final var internal = entitySetManager.getEntitySetsForOrganization( organizationId );
        final var external = authorizations.getAuthorizedObjectsOfType(
                orgPrincipal.getPrincipal(),
                SecurableObjectType.EntitySet,
                EnumSet.of( Permission.MATERIALIZE ) );
        final var materialized = assembler.getMaterializedEntitySetsInOrganization( organizationId );

        final Map<UUID, Set<OrganizationEntitySetFlag>> entitySets = new HashMap<>( 2 * internal.size() );

        if ( flagFilter.contains( OrganizationEntitySetFlag.INTERNAL ) ) {
            internal.forEach( entitySetId -> entitySets
                    .merge( entitySetId, EnumSet.of( OrganizationEntitySetFlag.INTERNAL ), ( lhs, rhs ) -> {
                        lhs.addAll( rhs );
                        return lhs;
                    } ) );

        }
        if ( flagFilter.contains( OrganizationEntitySetFlag.EXTERNAL ) ) {
            external.map( aclKey -> aclKey.get( 0 ) ).forEach( entitySetId -> entitySets
                    .merge( entitySetId, EnumSet.of( OrganizationEntitySetFlag.EXTERNAL ), ( lhs, rhs ) -> {
                        lhs.addAll( rhs );
                        return lhs;
                    } ) );

        }

        if ( flagFilter.contains( OrganizationEntitySetFlag.MATERIALIZED )
                || flagFilter.contains( OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED )
                || flagFilter.contains( OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED ) ) {
            materialized.forEach( ( entitySetId, flags ) -> {
                if ( flagFilter.contains( OrganizationEntitySetFlag.MATERIALIZED ) ) {
                    entitySets.merge( entitySetId, EnumSet.of( OrganizationEntitySetFlag.MATERIALIZED ),
                            ( lhs, rhs ) -> {
                                lhs.addAll( rhs );
                                return lhs;
                            } );
                }

                if ( flagFilter.contains( OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED )
                        && flags.contains( OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED ) ) {
                    entitySets.merge( entitySetId, EnumSet.of( OrganizationEntitySetFlag.EDM_UNSYNCHRONIZED ),
                            ( lhs, rhs ) -> {
                                lhs.addAll( rhs );
                                return lhs;
                            } );
                }

                if ( flagFilter.contains( OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED )
                        && flags.contains( OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED ) ) {
                    entitySets.merge( entitySetId, EnumSet.of( OrganizationEntitySetFlag.DATA_UNSYNCHRONIZED ),
                            ( lhs, rhs ) -> {
                                lhs.addAll( rhs );
                                return lhs;
                            } );
                }
            } );
        }

        return entitySets;
    }

    @Timed
    @Override
    @PostMapping(
            value = ID_PATH + ENTITY_SETS + ASSEMBLE,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Map<UUID, Set<OrganizationEntitySetFlag>> assembleEntitySets(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody Map<UUID, Integer> refreshRatesOfEntitySets ) {
        throw new NotImplementedException("DBT will fill this in.");
    }

    @Timed
    @Override
    @PostMapping( ID_PATH + ENTITY_SET_ID_PATH + SYNCHRONIZE )
    public Void synchronizeEdmChanges(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId ) {
        throw new NotImplementedException("DBT will fill this in.");
    }

    @Timed
    @Override
    @PostMapping( ID_PATH + ENTITY_SET_ID_PATH + REFRESH )
    public Void refreshDataChanges(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId ) {
        // the person requesting refresh should be the owner of the organization
        ensureOwner( organizationId );

        throw new NotImplementedException("DBT will fill this in.");
    }

    @Override
    @PutMapping( ID_PATH + ENTITY_SET_ID_PATH + REFRESH_RATE )
    public Void updateRefreshRate(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody Integer refreshRate ) {
        ensureOwner( organizationId );

        final var refreshRateInMilliSecs = getRefreshRateMillisFromMins( refreshRate );

        assembler.updateRefreshRate( organizationId, entitySetId, refreshRateInMilliSecs );
        return null;
    }

    @Override
    @DeleteMapping( ID_PATH + ENTITY_SET_ID_PATH + REFRESH_RATE )
    public Void deleteRefreshRate(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId
    ) {
        ensureOwner( organizationId );

        assembler.updateRefreshRate( organizationId, entitySetId, null );
        return null;
    }

    private Map<UUID, Map<UUID, PropertyType>> getAuthorizedPropertiesForMaterialization(
            UUID organizationId,
            Set<UUID> entitySetIds ) {
        // materialize should be a property level permission that can only be granted to organization principals and
        // the person requesting materialize should be the owner of the organization
        ensureOwner( organizationId );
        final var organizationPrincipal = organizations.getOrganizationPrincipal( organizationId );

        if ( organizationPrincipal == null ) {
            //This will be rare, since it is unlikely you have access to an organization that does not exist.
            throw new ResourceNotFoundException( "Organization does not exist." );
        }

        // check materialization on all linking and normal entity sets
        final var entitySets = entitySetManager.getEntitySetsAsMap( entitySetIds );
        final var allEntitySetIds = entitySets.values().stream()
                .flatMap( entitySet -> {
                    var entitySetIdsToCheck = Sets.newHashSet( entitySet.getId() );
                    if ( entitySet.isLinking() ) {
                        entitySetIdsToCheck.addAll( entitySet.getLinkedEntitySets() );
                    }

                    return entitySetIdsToCheck.stream();
                } ).collect( Collectors.toList() );

        allEntitySetIds.forEach( entitySetId -> ensureMaterialize( entitySetId, organizationPrincipal ) );

        // first we collect authorized property types of normal entity sets and then for each linking entity set, we
        // check materialization on normal entity sets and get the intersection of their authorized property types
        return authzHelper.getAuthorizedPropertiesOnEntitySets(
                entitySetIds, EnumSet.of( Permission.MATERIALIZE ), Set.of( organizationPrincipal.getPrincipal() ) );
    }

    private Long getRefreshRateMillisFromMins( Integer refreshRateInMins ) {
        if ( refreshRateInMins < 1 ) {
            throw new IllegalArgumentException( "Minimum refresh rate is 1 minute." );
        }

        // convert mins to millisecs
        return refreshRateInMins.longValue() * 3600L;
    }

    @Timed
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

    @Timed
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

    @Timed
    @Override
    @GetMapping(
            value = ID_PATH + EMAIL_DOMAINS,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Set<String> getAutoApprovedEmailDomains( @PathVariable( ID ) UUID organizationId ) {
        ensureOwner( organizationId );
        return organizations.getEmailDomains( organizationId );
    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + EMAIL_DOMAINS,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void setAutoApprovedEmailDomain(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody Set<String> emailDomains ) {
        ensureAdminAccess();
        organizations.setEmailDomains( organizationId, emailDomains );
        return null;
    }

    @Timed
    @Override
    @PostMapping(
            value = ID_PATH + EMAIL_DOMAINS,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Void addEmailDomains(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody Set<String> emailDomains ) {
        ensureAdminAccess();
        organizations.addEmailDomains( organizationId, emailDomains );
        return null;
    }

    @Timed
    @Override
    @DeleteMapping(
            value = ID_PATH + EMAIL_DOMAINS,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void removeEmailDomains(
            @PathVariable( ID ) UUID organizationId,
            @RequestBody Set<String> emailDomains ) {
        ensureAdminAccess();
        organizations.removeEmailDomains( organizationId, emailDomains );
        return null;
    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + EMAIL_DOMAINS + EMAIL_DOMAIN_PATH )
    public Void addEmailDomain(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( EMAIL_DOMAIN ) String emailDomain ) {
        ensureAdminAccess();
        organizations.addEmailDomains( organizationId, ImmutableSet.of( emailDomain ) );
        return null;
    }

    @Timed
    @Override
    @DeleteMapping(
            value = ID_PATH + EMAIL_DOMAINS + EMAIL_DOMAIN_PATH )
    public Void removeEmailDomain(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( EMAIL_DOMAIN ) String emailDomain ) {
        ensureAdminAccess();
        organizations.removeEmailDomains( organizationId, ImmutableSet.of( emailDomain ) );
        return null;
    }

    @Timed
    @Override
    @GetMapping(
            value = ID_PATH + PRINCIPALS + MEMBERS )
    public Iterable<OrganizationMember> getMembers( @PathVariable( ID ) UUID organizationId ) {
        ensureRead( organizationId );
        Set<Principal> members = organizations.getMembers( organizationId );
        Collection<SecurablePrincipal> securablePrincipals = principalService.getSecurablePrincipals( members );
        return securablePrincipals
                .stream()
                .map( sp -> new OrganizationMember( sp,
                        principalService.getUser( sp.getName() ),
                        principalService.getAllPrincipals( sp ) ) )::iterator;

    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + PRINCIPALS + MEMBERS + USER_ID_PATH )
    public Void addMember(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( USER_ID ) String userId ) {
        ensureOwnerAccess( new AclKey( organizationId ) );
        organizations.addMembers( organizationId, ImmutableSet.of( new Principal( PrincipalType.USER, userId ) ) );
        return null;
    }

    @Timed
    @Override
    @DeleteMapping(
            value = ID_PATH + PRINCIPALS + MEMBERS + USER_ID_PATH )
    public Void removeMember(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( USER_ID ) String userId ) {
        ensureOwnerAccess( new AclKey( organizationId ) );
        organizations.removeMembers( organizationId, ImmutableSet.of( new Principal( PrincipalType.USER, userId ) ) );
        edms.revokeAllPrivilegesFromMember( organizationId, userId );
        return null;
    }

    @Timed
    @Override
    @PostMapping(
            value = ROLES,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public UUID createRole( @RequestBody Role role ) {
        ensureOwner( role.getOrganizationId() );
        //We only create the role, but do not necessarily assign it to ourselves.
        organizations.createRoleIfNotExists( Principals.getCurrentUser(), role );
        return role.getId();
    }

    @Timed
    @Override
    @GetMapping(
            value = ID_PATH + PRINCIPALS + ROLES,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Set<Role> getRoles( @PathVariable( ID ) UUID organizationId ) {
        ensureRead( organizationId );
        Set<Role> roles = organizations.getRoles( organizationId );
        Set<AclKey> authorizedRoleAclKeys = getAuthorizedRoleAclKeys( roles );
        return Sets.filter( roles, role -> authorizedRoleAclKeys.contains( role.getAclKey() ) );
    }

    private Set<AclKey> getAuthorizedRoleAclKeys( Set<Role> roles ) {
        return authorizations
                .accessChecksForPrincipals( roles.stream()
                        .map( role -> new AccessCheck( role.getAclKey(), EnumSet.of( Permission.READ ) ) )
                        .collect( Collectors.toSet() ), Principals.getCurrentPrincipals() )
                .filter( authorization -> authorization.getPermissions().get( Permission.READ ) )
                .map( Authorization::getAclKey ).collect( Collectors.toSet() );
    }

    @Timed
    @Override
    @GetMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Role getRole( @PathVariable( ID ) UUID organizationId, @PathVariable( ROLE_ID ) UUID roleId ) {
        AclKey aclKey = new AclKey( organizationId, roleId );
        if ( isAuthorized( Permission.READ ).test( aclKey ) ) {
            return principalService.getRole( organizationId, roleId );
        } else {
            throw new ForbiddenException( "Unable to find role: " + aclKey );
        }
    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + TITLE,
            consumes = MediaType.TEXT_PLAIN_VALUE )
    public Void updateRoleTitle(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId,
            @RequestBody String title ) {
        ensureRoleAdminAccess( organizationId, roleId );
        //TODO: Do this in a less crappy way
        principalService.updateTitle( new AclKey( organizationId, roleId ), title );
        return null;
    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + DESCRIPTION,
            consumes = MediaType.TEXT_PLAIN_VALUE )
    public Void updateRoleDescription(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId,
            @RequestBody String description ) {
        ensureRoleAdminAccess( organizationId, roleId );
        principalService.updateDescription( new AclKey( organizationId, roleId ), description );
        return null;
    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + GRANT,
            consumes = MediaType.APPLICATION_JSON_VALUE
    )
    public Void updateRoleGrant(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId,
            @RequestBody Grant grant ) {
        ensureRoleAdminAccess( organizationId, roleId );
        organizations.updateRoleGrant( organizationId, roleId, grant );
        return null;
    }

    @Timed
    @Override
    @DeleteMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH )
    public Void deleteRole( @PathVariable( ID ) UUID organizationId, @PathVariable( ROLE_ID ) UUID roleId ) {
        ensureRoleAdminAccess( organizationId, roleId );
        ensureObjectCanBeDeleted( roleId );
        principalService.deletePrincipal( new AclKey( organizationId, roleId ) );
        return null;
    }

    @Timed
    @Override
    @GetMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<User> getAllUsersOfRole(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId ) {
        ensureRead( organizationId );
        return principalService.getAllUserProfilesWithPrincipal( new AclKey( organizationId, roleId ) );
    }

    @Timed
    @Override
    @PutMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS + USER_ID_PATH )
    public Void addRoleToUser(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId,
            @PathVariable( USER_ID ) String userId ) {
        ensureOwnerAccess( new AclKey( organizationId, roleId ) );

        organizations.addRoleToPrincipalInOrganization( organizationId, roleId,
                new Principal( PrincipalType.USER, userId ) );
        return null;
    }

    @Timed
    @Override
    @DeleteMapping(
            value = ID_PATH + PRINCIPALS + ROLES + ROLE_ID_PATH + MEMBERS + USER_ID_PATH )
    public Void removeRoleFromUser(
            @PathVariable( ID ) UUID organizationId,
            @PathVariable( ROLE_ID ) UUID roleId,
            @PathVariable( USER_ID ) String userId ) {
        ensureOwnerAccess( new AclKey( organizationId, roleId ) );

        organizations.removeRoleFromUser( new AclKey( organizationId, roleId ),
                new Principal( PrincipalType.USER, userId ) );
        return null;
    }

    @Override
    @Timed
    @PostMapping( value = ID_PATH + CONNECTIONS, consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void addConnections( @PathVariable( ID ) UUID organizationId, @RequestBody Set<String> connections ) {
        ensureAdminAccess();
        organizations.addConnections( organizationId, connections );
        return null;
    }

    @Timed
    @PutMapping( value = ID_PATH + CONNECTIONS, consumes = MediaType.APPLICATION_JSON_VALUE )
    @Override
    public Void setConnections( @PathVariable( ID ) UUID organizationId, @RequestBody Set<String> connections ) {
        ensureAdminAccess();
        organizations.setConnections( organizationId, connections );
        return null;
    }

    @Timed
    @DeleteMapping( value = ID_PATH + CONNECTIONS, consumes = MediaType.APPLICATION_JSON_VALUE )
    @Override
    public Void removeConnections( @PathVariable( ID ) UUID organizationId, @RequestBody Set<String> connections ) {
        organizations.removeConnections( organizationId, connections );
        return null;
    }

    private void ensureRoleAdminAccess( UUID organizationId, UUID roleId ) {
        ensureOwner( organizationId );

        AclKey aclKey = new AclKey( organizationId, roleId );
        accessCheck( aclKey, EnumSet.of( Permission.OWNER ) );
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

    private AclKey ensureOwner( UUID organizationId ) {
        AclKey aclKey = new AclKey( organizationId );
        accessCheck( aclKey, EnumSet.of( Permission.OWNER ) );
        return aclKey;
    }

    private void ensureRead( UUID organizationId ) {
        ensureReadAccess( new AclKey( organizationId ) );
    }

    private void ensureMaterialize( UUID entitySetId, OrganizationPrincipal principal ) {
        AclKey aclKey = new AclKey( entitySetId );

        if ( !getAuthorizationManager().checkIfHasPermissions(
                aclKey,
                Set.of( principal.getPrincipal() ),
                EnumSet.of( Permission.MATERIALIZE ) ) ) {
            throw new ForbiddenException( "EntitySet " + aclKey.toString() + " is not accessible by organization " +
                    "principal " + principal.getPrincipal().getId() + " ." );
        }
    }

}
