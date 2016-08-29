package com.kryptnostic.datastore.edm;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.geekbeast.rhizome.tests.pods.CassandraTestPod;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils.Pods;
import com.kryptnostic.types.Datastore;
import com.kryptnostic.types.pods.DatastoreServicesPod;
import com.kryptnostic.types.pods.DatastoreStreamSerializersPod;
import com.kryptnostic.types.pods.DatastoreTypeCodecsPod;

public class DatastoreServices extends RhizomeApplicationServer {
    @Configuration
    @Import( CassandraTestPod.class )
    static class DatastoreServicesTestPod extends DatastoreServicesPod {

    }

    public static final Class<?>[] datastorePods = new Class<?>[] {
            DatastoreServicesTestPod.class,
            DatastoreTypeCodecsPod.class, DatastoreStreamSerializersPod.class
    };

    public DatastoreServices( Class<?>... pods ) {
        super(
                Pods.concatenate( pods,
                        Datastore.rhizomePods,
                        RhizomeApplicationServer.defaultPods,
                        datastorePods ) );
    }
}
