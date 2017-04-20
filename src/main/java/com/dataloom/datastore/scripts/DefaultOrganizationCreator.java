package com.dataloom.datastore.scripts;

import com.dataloom.authorization.SystemRole;
import com.dataloom.edm.internal.DatastoreConstants;
import com.dataloom.organization.Organization;
import com.dataloom.organization.roles.OrganizationRole;
import com.dataloom.organizations.HazelcastOrganizationService;
import com.dataloom.organizations.roles.RolesManager;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

public class DefaultOrganizationCreator {
    private HazelcastOrganizationService orgs;
    private RolesManager rm;
    
    private static final String DEFAULT_ORGANIZATION_NAME = "Loom";
    
    public DefaultOrganizationCreator( HazelcastOrganizationService orgs, RolesManager rm ){
        this.orgs = orgs;
        this.rm = rm;
    }
    
    public void run(){
        /*
         * Create default organization
         */
        Organization organization = new Organization( Optional.of( DatastoreConstants.DEFAULT_ORGANIZATION_ID ),
                DEFAULT_ORGANIZATION_NAME,
                Optional.absent(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of()
                );
        orgs.createOrganization( organization );
        
        /*
         * Create default roles
         */
        for( SystemRole r : SystemRole.values() ){
            OrganizationRole role = new OrganizationRole( Optional.absent(), DatastoreConstants.DEFAULT_ORGANIZATION_ID, r.getName(), Optional.absent() );
            rm.createRoleIfNotExists( role );
        }
    }
}
