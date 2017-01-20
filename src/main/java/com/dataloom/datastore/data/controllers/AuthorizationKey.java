package com.dataloom.datastore.data.controllers;

import java.util.UUID;

import com.dataloom.authorization.Principal;

public class AuthorizationKey {
    private final Principal user;
    private final UUID      entitySetId;
    private final UUID      syncId;

    public AuthorizationKey( Principal user, UUID entitySetId, UUID syncId ) {
        this.user = user;
        this.entitySetId = entitySetId;
        this.syncId = syncId;
    }

    public Principal getUser() {
        return user;
    }

    public UUID getEntitySetId() {
        return entitySetId;
    }

    public UUID getSyncId() {
        return syncId;
    }

}
