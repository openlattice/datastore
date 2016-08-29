package com.kryptnostic.types.services;

import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.NotImplementedException;

public class DatasourceManager {
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
