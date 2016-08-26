package com.kryptnostic.datastore.edm;

import com.geekbeast.rhizome.tests.pods.CassandraTestPod;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils.Pods;
import com.kryptnostic.types.Datastore;

public class DatastoreServices extends RhizomeApplicationServer {
    public DatastoreServices() {
        super(
                Pods.concatenate( Datastore.servicePods,
                        RhizomeApplicationServer.defaultPods,
                        new Class<?>[] { CassandraTestPod.class } ) );
    }
}
