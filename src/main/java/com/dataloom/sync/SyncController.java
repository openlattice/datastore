package com.dataloom.sync;

import java.util.EnumSet;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.ForbiddenException;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principals;
import com.dataloom.datastore.services.DatasourceManager;
import com.google.common.collect.ImmutableList;

@RestController
@RequestMapping( SyncApi.CONTROLLER )
public class SyncController implements SyncApi {

    @Inject
    private AuthorizationManager authz;

    @Inject
    private DatasourceManager    datasourceManager;

    @Override
    @RequestMapping(
        path = { ENTITY_SET_ID_PATH + NEW },
        method = RequestMethod.GET,
        consumes = MediaType.APPLICATION_JSON_VALUE )
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
        path = { ENTITY_SET_ID_PATH },
        method = RequestMethod.GET,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public UUID getLatestSyncId( @PathVariable UUID entitySetId ) {
        if ( authz.checkIfHasPermissions( ImmutableList.of( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            return datasourceManager.getLatestSyncId( entitySetId );
        } else {
            throw new ForbiddenException(
                    "Insufficient permissions to read the sync id of the entity set or it doesn't exist." );
        }
    }
}