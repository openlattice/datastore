package com.dataloom.datastore.services;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.ForbiddenException;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.data.TicketKey;
import com.dataloom.hazelcast.HazelcastMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.Util;
import com.kryptnostic.rhizome.hazelcast.objects.DelegatedUUIDSet;;

public class SyncTicketService {
    private final IMap<TicketKey, UUID>             authorizedEntitySets;
    private final IMap<TicketKey, DelegatedUUIDSet> authorizedProperties;

    public SyncTicketService( HazelcastInstance hazelcast ) {
        authorizedEntitySets = hazelcast.getMap( HazelcastMap.ENTITY_SET_TICKETS.name() );
        authorizedProperties = hazelcast.getMap( HazelcastMap.ENTITY_SET_PROPERTIES_TICKETS.name() );
    }

    public UUID acquireTicket( Principal principal, UUID entitySetId, Set<UUID> propertyIds ) {
        checkArgument( principal.getType().equals( PrincipalType.USER ), "Only users can retrieve tickets." );
        TicketKey key = new TicketKey( principal.getId() );

        while ( authorizedEntitySets.putIfAbsent( key, entitySetId, 24, TimeUnit.HOURS ) != null ) {
            key = new TicketKey( principal.getId() );
        }
        authorizedProperties.set( key, DelegatedUUIDSet.wrap( propertyIds ), 24, TimeUnit.HOURS );
        return key.getTicket();
    }

    public void releaseTicket( Principal principal, UUID ticketId ) {
        TicketKey key = ticketKey( principal, ticketId );
        authorizedEntitySets.delete( key );
        authorizedProperties.delete( key );
    }

    private TicketKey ticketKey( Principal principal, UUID ticketId ) {
        return new TicketKey( checkNotNull( principal ).getId(), checkNotNull( ticketId ) );
    }

    public Set<UUID> getAuthorizedProperties( Principal principal, UUID ticket ) {
        TicketKey key = ticketKey( principal, ticket );
        if ( authorizedEntitySets.containsKey( key ) ) {
            return Util.getSafely( authorizedProperties, key ).unwrap();
        }
        throw new ForbiddenException( "Unable to authorized access to resource" );
    }

    public UUID getAuthorizedEntitySet( Principal currentUser, UUID ticket ) {
        UUID entitySetId = Util.getSafely( authorizedEntitySets, ticketKey( currentUser, ticket ) );
        if ( entitySetId == null ) {
            throw new ForbiddenException( "Unable to authorized access to resource" );
        } else {
            return entitySetId;
        }
    }
}
