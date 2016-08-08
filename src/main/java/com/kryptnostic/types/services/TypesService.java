package com.kryptnostic.types.services;

import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.mapstores.v2.constants.HazelcastNames.Maps;
import com.kryptnostic.types.entryprocessors.GetOrCreateUuidForTypeEntryProcessor;
import com.kryptnostic.v2.storage.models.Scope;

public class TypesService {
    private final IMap<String, Scope> scopes;

    @Inject
    public TypesService( HazelcastInstance hazelcastInstance ) {
        this.scopes = hazelcastInstance.getMap( Maps.SCOPES );
    }

    public Map<String, Scope> getScopes() {
        if ( scopes.isEmpty() ) {
            return ImmutableMap.of();
        }

        return scopes.getAll( scopes.keySet() );
    }

    public Scope getScopeInformation( String scope ) {
        return scopes.get( scope );
    }

    public UUID getOrCreateUuidForType( String scope, String type ) {
        Object res = scopes.executeOnKey( scope, new GetOrCreateUuidForTypeEntryProcessor( type ) );
        return (UUID) res;
    }

}