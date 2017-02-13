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
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.linking.LinkingApi;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.kryptnostic.datastore.services.EdmManager;

public class LinkingController implements LinkingApi, AuthorizingComponent {

    @Inject
    private AuthorizationManager authz;

    @Inject
    private EdmManager           dms;

    @Inject
    private LinkingService       linkingService;

    @Override
    public UUID linkEntitySets( Map<UUID, UUID> entitySetsWithSyncIds, Set<Map<UUID, UUID>> linkingProperties ) {
        // Validate, compute the set of property types after merging - by default PII fields are left out.
        Multimap<UUID, UUID> linkingMap = validateAndGetLinkingMultimap( entitySetsWithSyncIds, linkingProperties );

        return linkingService.executeLinking( entitySetsWithSyncIds, linkingMap, linkingProperties );
    }

    @Override
    public UUID linkEntities( UUID syncId, UUID entitySetId, UUID entityId, Set<EntityKey> entities ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void setLinkedEntities( UUID syncId, UUID entitySetId, UUID entityId, Set<EntityKey> entities ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void deleteLinkedEntities( UUID syncId, UUID entitySetId, UUID entityId ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void addLinkedEntities( UUID syncId, UUID entitySetId, UUID entityId, UUID linkedEntityId ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void removeLinkedEntity( UUID syncId, UUID entitySetId, UUID entityId, UUID linkedEntityId ) {
        // TODO Auto-generated method stub
        return null;
    }

    private Multimap<UUID, UUID> validateAndGetLinkingMultimap( Map<UUID, UUID> entitySetsWithSyncIds, Set<Map<UUID, UUID>> linkingProperties ) {
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
        
        Preconditions.checkArgument( linkingES.equals( entitySetsWithSyncIds.keySet() ), "Entity Sets and Linking Properties are not compatible." );
        
        // Compute the set of property types needed for each entity set + the property types needed after merging.
        // Select the entity set with linked properties, read them in spark.
        Multimap<UUID, UUID> readablePropertiesMap = HashMultimap.create();
        for ( UUID esId : linkingES ) {
            // add readable properties
            EntitySet es = dms.getEntitySet( esId );
            EntityType et = dms.getEntityType( es.getEntityTypeId() );
            dms.getPropertyTypes( et.getProperties() ).stream().filter( pt -> {
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

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authz;
    }

}
