package com.kryptnostic.types;

import com.kryptnostic.mapstores.pods.BaseSerializersPod;
import com.kryptnostic.rhizome.configuration.websockets.BaseRhizomeServer;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils.Pods;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod;
import com.kryptnostic.types.pods.DataStoreSecurityPod;
import com.kryptnostic.types.pods.DatastoreServicesPod;
import com.kryptnostic.types.pods.DatastoreServletsPod;
import com.kryptnostic.types.pods.DatastoreStreamSerializersPod;
import com.kryptnostic.types.pods.DatastoreTypeCodecsPod;

public class Datastore extends BaseRhizomeServer {
    public static final Class<?>[] webPods       = new Class<?>[] { DatastoreServletsPod.class,
            DataStoreSecurityPod.class, };
    public static final Class<?>[] rhizomePods   = new Class<?>[] {
            CassandraPod.class,
            BaseSerializersPod.class,
            RegistryBasedHazelcastInstanceConfigurationPod.class };
    public static final Class<?>[] datastorePods = new Class<?>[] {
            DatastoreServicesPod.class,
            DatastoreTypeCodecsPod.class, DatastoreStreamSerializersPod.class
    };

    public Datastore( Class<?>... pods ) {
        super( Pods.concatenate( pods, webPods, rhizomePods, RhizomeApplicationServer.defaultPods ) );
    }

    public static void main( String[] args ) throws Exception {
        new Datastore().start( args );
    }
}
