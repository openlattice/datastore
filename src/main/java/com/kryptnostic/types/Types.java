package com.kryptnostic.types;

import com.kryptnostic.mapstores.pods.BaseSerializersPod;
import com.kryptnostic.rhizome.configuration.websockets.BaseRhizomeServer;
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod;
import com.kryptnostic.types.pods.TypesSecurityPod;
import com.kryptnostic.types.pods.TypesServicesPod;
import com.kryptnostic.types.pods.TypesServletsPod;

public class Types extends BaseRhizomeServer {
    public Types( Class<?>... defaultPods ) {
        super(
                RegistryBasedHazelcastInstanceConfigurationPod.class,
                BaseSerializersPod.class,
                TypesServletsPod.class,
                TypesServicesPod.class,
                TypesSecurityPod.class );
    }

    public static void main( String[] args ) throws Exception {
        new Types().start( args );
    }

}
