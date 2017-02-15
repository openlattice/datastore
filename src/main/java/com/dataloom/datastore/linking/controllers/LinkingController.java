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
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principals;
import com.dataloom.data.EntityKey;
import com.dataloom.datastore.services.LinkingService;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.set.LinkingEntitySet;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.LinkingEntityType;
import com.dataloom.linking.HazelcastListingService;
import com.dataloom.linking.LinkingApi;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.kryptnostic.datastore.services.EdmManager;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import retrofit2.http.Body;
import retrofit2.http.Path;

import javax.inject.Inject;
import java.util.*;
import java.util.Map.Entry;

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
        Set<Map<UUID, UUID>> linkingProperties = linkingEntitySet.getLinkingProperties();

        // Validate, compute the ownable property types after merging - by default PII fields are left out.
        Set<UUID> ownablePropertyTypes = validateAndGetOwnablePropertyTypes( linkingProperties );

        EntitySet entitySet = linkingEntitySet.getEntitySet();
        edm.createEntitySet( Principals.getCurrentUser(), entitySet, ownablePropertyTypes );
        UUID linkedEntitySetId = entitySet.getId();

        return linkingService.link( linkedEntitySetId, linkingProperties );
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

    private Set<UUID> validateAndGetOwnablePropertyTypes( Set<Map<UUID, UUID>> linkingProperties ) {

        //Validate: each map in the set should have a unique value, which is distinct across the linking properties set.
        Set<UUID> linkingES = new HashSet<>();
        Set<UUID> validatedProperties = new HashSet<>();
        SetMultimap<UUID, UUID> linkIndexedByProperties = HashMultimap.create();
        
        linkingProperties.stream().forEach( link -> {
            Preconditions.checkArgument( link.values().size() == 1, "Each linking map should involve a unique property type." );
            Preconditions.checkArgument( link.entrySet().size() > 1, "Each linking map must be matching at least two entity sets." );
            //Get the value of common property type id in the linking map.
            UUID propertyId = link.values().iterator().next();
            Preconditions.checkArgument( !validatedProperties.contains( propertyId ), "There should be only one linking map that involves property id " + propertyId );

            for( UUID esId : link.keySet() ){
                List<UUID> aclKey = Arrays.asList( esId, propertyId );
                ensureLinkAccess( aclKey );
                linkingES.add( esId );
                linkIndexedByProperties.put( propertyId, esId );
            }
        });

        // Sanity check: authorized to link the entity set itself.
        linkingES.stream().forEach( entitySetId -> ensureLinkAccess( Arrays.asList( entitySetId ) ) );

        // Compute the ownable property types after merging. A property type is ownable if calling user has both READ and LINK permissions for that property type in all the entity sets involved.
        Set<UUID> ownablePropertyTypes = new HashSet<>();
        for( UUID propertyId : linkIndexedByProperties.keySet() ){
            Set<UUID> entitySets = linkIndexedByProperties.get( propertyId );
            
            boolean ownable = entitySets.stream().map( esId -> Arrays.asList( esId, propertyId ) ).allMatch( isAuthorized( Permission.LINK, Permission.READ ) );
            
            if( ownable ){
                ownablePropertyTypes.add( propertyId );
            }
        }
        if ( ownablePropertyTypes.isEmpty() ) {
            throw new IllegalArgumentException( "There will be no ownable properties in the linked entity set." );
        }

        return ownablePropertyTypes;
    }

}
