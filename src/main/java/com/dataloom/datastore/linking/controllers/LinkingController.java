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

package com.dataloom.datastore.linking.controllers;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.data.EntityKey;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.LinkingEntityType;
import com.dataloom.linking.HazelcastListingService;
import com.dataloom.linking.LinkingApi;
import com.kryptnostic.datastore.services.EdmManager;
import org.springframework.web.bind.annotation.*;
import retrofit2.http.Body;
import retrofit2.http.Path;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
@RestController
@RequestMapping( LinkingApi.CONTROLLER )
public class LinkingController implements LinkingApi, AuthorizingComponent {

    @Inject
    private AuthorizationManager authorizationManager;

    @Inject
    private EdmManager edm;

    @Inject
    private HazelcastListingService listings;

    @Override
    @PostMapping( "/" + TYPE )
    public UUID createLinkingEntityType( @RequestBody LinkingEntityType linkingEntityType ) {
        EntityType entityType = linkingEntityType.getLinkingEntityType();
        edm.createEntityType( entityType );
        listings.setLinkedEntityTypes( entityType.getId(), linkingEntityType.getLinkedEntityTypes() );
        return entityType.getId();
    }

    @Override
    @PostMapping( "/" + SET )
    public UUID linkEntitySets(
            @RequestParam( TYPE ) UUID linkingEntityType, @RequestBody Set<Map<UUID, UUID>> linkingProperties ) {
        return null;
    }

    @Override public UUID linkEntities(
            @Path( SYNC_ID ) UUID syncId,
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_ID ) UUID entityId,
            @Body Set<EntityKey> entities ) {
        return null;
    }

    @Override public Void setLinkedEntities(
            @Path( SYNC_ID ) UUID syncId,
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_ID ) UUID entityId,
            @Body Set<EntityKey> entities ) {
        return null;
    }

    @Override public Void deleteLinkedEntities(
            @Path( SYNC_ID ) UUID syncId, @Path( SET_ID ) UUID entitySetId, @Path( ENTITY_ID ) UUID entityId ) {
        return null;
    }

    @Override public Void addLinkedEntities(
            @Path( SYNC_ID ) UUID syncId,
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_ID ) UUID entityId,
            @Path( LINKED_ENTITY_ID ) UUID linkedEntityId ) {
        return null;
    }

    @Override public Void removeLinkedEntity(
            @Path( SYNC_ID ) UUID syncId,
            @Path( SET_ID ) UUID entitySetId,
            @Path( ENTITY_ID ) UUID entityId,
            @Path( LINKED_ENTITY_ID ) UUID linkedEntityId ) {
        return null;
    }

    @Override public AuthorizationManager getAuthorizationManager() {
        return authorizationManager;
    }
}
