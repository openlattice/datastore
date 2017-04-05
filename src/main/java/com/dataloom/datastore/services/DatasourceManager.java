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

package com.dataloom.datastore.services;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.lang3.NotImplementedException;

import com.dataloom.edm.type.PropertyType;
import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.sync.events.CurrentSyncUpdatedEvent;
import com.dataloom.sync.events.SyncIdCreatedEvent;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.services.CassandraDataManager;
import com.kryptnostic.datastore.services.EdmService;

public class DatasourceManager {

    @Inject
    private CassandraDataManager   cdm;

    @Inject
    private EdmService             dms;

    @Inject
    private EventBus               eventBus;

    private final IMap<UUID, UUID> currentSyncIds;

    public DatasourceManager( HazelcastInstance hazelcastInstance ) {
        this.currentSyncIds = hazelcastInstance.getMap( HazelcastMap.SYNC_IDS.name() );
    }

    public UUID getCurrentSyncId( UUID entitySetId ) {
        return currentSyncIds.get( entitySetId );
    }

    public void setCurrentSyncId( UUID entitySetId, UUID syncId ) {
        currentSyncIds.put( entitySetId, syncId );
        eventBus.post( new CurrentSyncUpdatedEvent( entitySetId, syncId ) );
    }

    public UUID createNewSyncIdForEntitySet( UUID entitySetId ) {
        UUID newSyncId = UUIDs.timeBased();
        cdm.addSyncIdToEntitySet( entitySetId, newSyncId );
        
        List<PropertyType> propertyTypes = Lists.newArrayList( dms.getPropertyTypes(
                dms.getEntityType( dms.getEntitySet( entitySetId ).getEntityTypeId() ).getProperties() ) );
        eventBus.post( new SyncIdCreatedEvent( entitySetId, newSyncId, propertyTypes ) );
        return newSyncId;
    }
    
    public UUID getLatestSyncId( UUID entitySetId ) {
        return cdm.getMostRecentSyncIdForEntitySet( entitySetId );
    }

    public UUID createDatasource( UUID aclId, String name, String description, UUID syncId ) {
        throw new NotImplementedException( "MTR WAS HERE." );
    }

    public UUID initializeSync( UUID datasourceId ) {
        throw new NotImplementedException( "MTR WAS HERE." );
    }

    public void finalizeSync( UUID syncId ) {
        throw new NotImplementedException( "MTR WAS HERE." );
    }

    public Set<UUID> getOutstandingSyncs( UUID datasourceId ) {
        throw new NotImplementedException( "MTR WAS HERE." );
    }
}
