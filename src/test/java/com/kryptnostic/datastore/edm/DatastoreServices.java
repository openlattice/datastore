package com.kryptnostic.datastore.edm;

import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.types.Datastore;

public class DatastoreServices extends RhizomeApplicationServer {
    public DatastoreServices() {
        super( Datastore.servicePods );
    }
}
