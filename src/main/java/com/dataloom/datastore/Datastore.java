package com.dataloom.datastore;

import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.data.serializers.FullQualifedNameJacksonSerializer;
import com.dataloom.datastore.pods.DatastoreSecurityPod;
import com.dataloom.datastore.pods.DatastoreServicesPod;
import com.dataloom.datastore.pods.DatastoreServletsPod;
import com.dataloom.hazelcast.pods.MapstoresPod;
import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.kryptnostic.conductor.codecs.pods.TypeCodecsPod;
import com.kryptnostic.conductor.rpc.SharedStreamSerializersPod;
import com.kryptnostic.datastore.cassandra.CassandraTablesPod;
import com.kryptnostic.rhizome.configuration.websockets.BaseRhizomeServer;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils.Pods;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod;

import digital.loom.rhizome.authentication.Auth0Pod;

public class Datastore extends BaseRhizomeServer {
    public static final Class<?>[] webPods       = new Class<?>[] {
            DatastoreServletsPod.class,
            DatastoreSecurityPod.class, };
    public static final Class<?>[] rhizomePods   = new Class<?>[] {
            CassandraPod.class,
            RegistryBasedHazelcastInstanceConfigurationPod.class,
            Auth0Pod.class };

    public static final Class<?>[] datastorePods = new Class<?>[] {
            DatastoreServicesPod.class,
            TypeCodecsPod.class, 
//            SharedStreamSerializersPod.class,
            MapstoresPod.class,
            CassandraTablesPod.class,
    };

    static {
        ObjectMappers.foreach( FullQualifedNameJacksonSerializer::registerWithMapper );
        ObjectMappers.foreach( FullQualifedNameJacksonDeserializer::registerWithMapper );
        ObjectMappers.foreach( mapper -> mapper.disable( SerializationFeature.WRITE_DATES_AS_TIMESTAMPS ) );
    }

    public Datastore( Class<?>... pods ) {
        super( Pods.concatenate(
                pods,
                webPods,
                rhizomePods,
                RhizomeApplicationServer.defaultPods,
                datastorePods ) );
    }

    public static void main( String[] args ) throws Exception {
        new Datastore().start( args );
    }
}
