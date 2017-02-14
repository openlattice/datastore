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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.authorization.Permission;
import com.dataloom.data.EntityKey;
import com.dataloom.datastore.services.LinkingService;
import com.dataloom.edm.EntitySet;
<<<<<<< HEAD
import com.dataloom.edm.set.LinkingEntitySet;
=======
>>>>>>> fa5d859721b504133fd5ea5415d2eff9dd1d4984
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.LinkingEntityType;
import com.dataloom.linking.HazelcastListingService;
import com.dataloom.linking.LinkingApi;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.kryptnostic.datastore.services.EdmManager;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import retrofit2.http.Body;
import retrofit2.http.Path;

<<<<<<< HEAD
import javax.inject.Inject;
import java.util.*;

=======
>>>>>>> fa5d859721b504133fd5ea5415d2eff9dd1d4984
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
    
    @Inject
    private LinkingService       linkingService;


    @Inject
    private LinkingService linkingService;

    @Override
    @PostMapping( value = "/"
            + TYPE, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE )
    public UUID createLinkingEntityType( @RequestBody LinkingEntityType linkingEntityType ) {
        EntityType entityType = linkingEntityType.getLinkingEntityType();
        edm.createEntityType( entityType );
        listings.setLinkedEntityTypes( entityType.getId(), linkingEntityType.getLinkedEntityTypes() );
        return entityType.getId();
    }

    @Override
    @PostMapping( value = "/"
            + SET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE )
    public UUID linkEntitySets( @RequestBody LinkingEntitySet linkingEntitySet ) {
        EntitySet entitySet = linkingEntitySet.getEntitySet();
        Set<Map<UUID, UUID>> linkingProperties = linkingEntitySet.getLinkingProperties();

        // Validate, compute the set of property types after merging - by default PII fields are left out.
        Multimap<UUID, UUID> linkingMap = validateAndGetLinkingMultimap( linkingProperties );

        return linkingService.link( linkingMap, linkingProperties );
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

    private Multimap<UUID, UUID> validateAndGetLinkingMultimap( Set<Map<UUID, UUID>> linkingProperties ) {

        Multimap<UUID, UUID> linkingMap = HashMultimap.create();

        linkingProperties.stream().map( m -> m.entrySet() )
                .forEach( set -> {
                    Preconditions.checkArgument( set.size() > 1,
                            "Map cannot have length 1, which does not match any properties." );
                    for ( Entry<UUID, UUID> entry : set ) {
                        List<UUID> aclKey = Arrays.asList( entry.getKey(), entry.getValue() );
                        ensureLinkAccess( aclKey );
                        // Add entity set id to linking Sets if nothing wrong happens so far
                        linkingMap.put( entry.getKey(), entry.getValue() );
                    }
                } );

        Set<UUID> linkingES = linkingMap.keySet();

        // Sanity check
        linkingES.stream().forEach( entitySetId -> ensureLinkAccess( Arrays.asList( entitySetId ) ) );

        // Compute the set of property types needed for each entity set + the property types needed after merging.
        // Select the entity set with linked properties, read them in spark.
        Multimap<UUID, UUID> readablePropertiesMap = HashMultimap.create();
        for ( UUID esId : linkingES ) {
            // add readable properties
            EntitySet es = edm.getEntitySet( esId );
            EntityType et = edm.getEntityType( es.getEntityTypeId() );
            edm.getPropertyTypes( et.getProperties() ).stream().filter( pt -> {
                List<UUID> aclKey = Arrays.asList( esId, pt.getId() );
                // By default, remove PII fields in linked entity set
                return isAuthorized( Permission.READ ).test( aclKey ) && !pt.isPIIfield();
            } ).forEach( pt -> readablePropertiesMap.put( esId, pt.getId() ) );
        }
        if ( readablePropertiesMap.isEmpty() ) {
            throw new IllegalArgumentException( "There will be no readable properties in the linked entity set." );
        }

        return linkingMap;
    }

}
