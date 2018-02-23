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

package com.dataloom.datastore.data.controllers;

import java.util.UUID;

import com.openlattice.authorization.Principal;

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
