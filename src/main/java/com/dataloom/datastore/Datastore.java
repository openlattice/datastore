/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.datastore;

import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.data.serializers.FullQualifedNameJacksonSerializer;
import com.dataloom.datastore.pods.DatastoreSecurityPod;
import com.dataloom.datastore.pods.DatastoreServicesPod;
import com.dataloom.datastore.pods.DatastoreServletsPod;
import com.dataloom.datastore.pods.SparkDependencyPod;
import com.dataloom.hazelcast.pods.MapstoresPod;
import com.dataloom.hazelcast.pods.SharedStreamSerializersPod;
import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.kryptnostic.conductor.codecs.pods.TypeCodecsPod;
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
            SharedStreamSerializersPod.class,
            MapstoresPod.class,
            CassandraTablesPod.class,
            SparkDependencyPod.class
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
                RhizomeApplicationServer.DEFAULT_PODS,
                datastorePods ) );
    }

    public static void main( String[] args ) throws Exception {
        new Datastore().start( args );
    }
}
