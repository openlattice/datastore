package com.dataloom.datastore.edm;

import com.dataloom.datastore.Datastore;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils.Pods;

public class DatastoreServices extends RhizomeApplicationServer {
    public DatastoreServices( Class<?>... pods ) {
        super(
                Pods.concatenate( pods,
                        Datastore.datastorePods,
                        Datastore.rhizomePods,
                        RhizomeApplicationServer.defaultPods ) );
    }
}
