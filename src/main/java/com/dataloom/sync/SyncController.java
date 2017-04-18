package com.dataloom.sync;

import java.util.EnumSet;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.authorization.ForbiddenException;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principals;
import com.google.common.collect.ImmutableList;
import com.dataloom.data.DatasourceManager;

@RestController
@RequestMapping( SyncApi.CONTROLLER )
public class SyncController implements SyncApi, AuthorizingComponent {

    @Inject
    private AuthorizationManager authz;

    @Inject
    private DatasourceManager    datasourceManager;

    @Override
    @RequestMapping(
        path = { ENTITY_SET_ID_PATH },
        method = RequestMethod.GET )
    public UUID acquireSyncId( @PathVariable UUID entitySetId ) {
        if ( authz.checkIfHasPermissions( ImmutableList.of( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.WRITE ) ) ) {
            return datasourceManager.createNewSyncIdForEntitySet( entitySetId );
        } else {
            throw new ForbiddenException(
                    "Insufficient permissions to acquire a sync id for this entity set or it doesn't exist." );
        }
    }

    @Override
    @RequestMapping(
        path = { ENTITY_SET_ID_PATH + CURRENT },
        method = RequestMethod.GET )
    public UUID getCurrentSyncId( @PathVariable UUID entitySetId ) {
        if ( authz.checkIfHasPermissions( ImmutableList.of( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            return datasourceManager.getCurrentSyncId( entitySetId );
        } else {
            throw new ForbiddenException(
                    "Insufficient permissions to read the sync id of the entity set or it doesn't exist." );
        }
    }

    @Override
    @RequestMapping(
        path = { ENTITY_SET_ID_PATH + SYNC_ID_PATH },
        method = RequestMethod.POST )
    public Void setCurrentSyncId( @PathVariable UUID entitySetId, @PathVariable UUID syncId ) {
        if ( authz.checkIfHasPermissions( ImmutableList.of( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.WRITE ) ) ) {
            datasourceManager.setCurrentSyncId( entitySetId, syncId );
            return null;
        } else {
            throw new ForbiddenException(
                    "Insufficient permissions to set the current sync id of the entity set or it doesn't exist." );
        }
    }

    @Override
    @RequestMapping(
        path = { ENTITY_SET_ID_PATH + LATEST },
        method = RequestMethod.GET )
    public UUID getLatestSyncId( @PathVariable UUID entitySetId ) {
        if ( authz.checkIfHasPermissions( ImmutableList.of( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            return datasourceManager.getLatestSyncId( entitySetId );
        } else {
            throw new ForbiddenException(
                    "Insufficient permissions to read the latest sync id of the entity set or it doesn't exist." );
        }
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authz;
    }
}