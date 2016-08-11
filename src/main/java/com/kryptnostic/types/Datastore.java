package com.kryptnostic.types;

import com.kryptnostic.mapstores.pods.BaseSerializersPod;
import com.kryptnostic.rhizome.configuration.websockets.BaseRhizomeServer;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod;
import com.kryptnostic.types.pods.DataStoreSecurityPod;
import com.kryptnostic.types.pods.DatastoreServicesPod;
import com.kryptnostic.types.pods.DatastoreServletsPod;
import com.kryptnostic.types.pods.DatastoreStreamSerializersPod;

public class Datastore extends BaseRhizomeServer {
    public Datastore( Class<?>... defaultPods ) {
        super(
                RegistryBasedHazelcastInstanceConfigurationPod.class,
                BaseSerializersPod.class,
                CassandraPod.class,
                DatastoreServletsPod.class,
                DatastoreServicesPod.class,
                DataStoreSecurityPod.class,
                DatastoreStreamSerializersPod.class );
    }

    public static void main( String[] args ) throws Exception {
        new Datastore().start( args );
    }

}
