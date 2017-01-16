package com.dataloom.datastore;

import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.data.serializers.FullQualifedNameJacksonSerializer;
import com.dataloom.datastore.pods.DatastoreSecurityPod;
import com.dataloom.datastore.pods.DatastoreServicesPod;
import com.dataloom.datastore.pods.DatastoreServletsPod;
import com.dataloom.datastore.pods.DatastoreStreamSerializersPod;
import com.dataloom.hazelcast.pods.MapstoresPod;
import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.kryptnostic.conductor.codecs.pods.TypeCodecsPod;
import com.kryptnostic.datastore.cassandra.CassandraTablesPod;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.rhizome.configuration.websockets.BaseRhizomeServer;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils.Pods;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod;

import digital.loom.rhizome.authentication.Auth0Pod;

public class Datastore extends BaseRhizomeServer {
    // @Deprecated
    // public static final String     ES_PRODUCTS_NAME = "Products";
    public static final Class<?>[] webPods          = new Class<?>[] { DatastoreServletsPod.class,
            DatastoreSecurityPod.class, };
    public static final Class<?>[] rhizomePods      = new Class<?>[] {
            CassandraPod.class,
            RegistryBasedHazelcastInstanceConfigurationPod.class,
            Auth0Pod.class };

    public static final Class<?>[] datastorePods    = new Class<?>[] {
            DatastoreServicesPod.class,
            TypeCodecsPod.class, DatastoreStreamSerializersPod.class,
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

    public static void init( EdmManager dms ) {
        //
        // final String ET_PRODUCT_NAME = "Product";
        // final FullQualifiedName ET_PRODUCT_FQN = new FullQualifiedName(
        // KryptnosticEdmProvider.NAMESPACE,
        // ET_PRODUCT_NAME );
        //
        // dms.createPropertyType( new PropertyType().setNamespace( KryptnosticEdmProvider.NAMESPACE ).setName( "ID" )
        // .setDatatype( EdmPrimitiveTypeKind.Int32 ).setMultiplicity( 0 ) );
        // dms.createPropertyType( new PropertyType().setNamespace( KryptnosticEdmProvider.NAMESPACE ).setName( "Name" )
        // .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 ) );
        // dms.createPropertyType(
        // new PropertyType().setNamespace( KryptnosticEdmProvider.NAMESPACE ).setName( "Description" )
        // .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 ) );
        // EntityType product = new EntityType()
        // .setNamespace( KryptnosticEdmProvider.NAMESPACE )
        // .setName( ET_PRODUCT_NAME )
        // .setKey( ImmutableSet.of( new FullQualifiedName( KryptnosticEdmProvider.NAMESPACE, "ID" ) ) )
        // .setProperties( ImmutableSet.of( new FullQualifiedName( KryptnosticEdmProvider.NAMESPACE, "ID" ),
        // new FullQualifiedName( KryptnosticEdmProvider.NAMESPACE, "Name" ),
        // new FullQualifiedName( KryptnosticEdmProvider.NAMESPACE, "Description" ) ) );
        //
        // dms.createEntityType( Principals.getCurrentUser(), product );
        // dms.createEntitySet( Principals.getCurrentUser(), ET_PRODUCT_FQN, ES_PRODUCTS_NAME, null );
        // dms.createEntitySet( Principals.getCurrentUser(), new FullQualifiedName( KryptnosticEdmProvider.NAMESPACE,
        // "metadataLevel" ),
        // "metadataLevels",
        // null );
        //
        // dms.createSchema( KryptnosticEdmProvider.NAMESPACE,
        // "agora",
        // ACLs.EVERYONE_ACL,
        // ImmutableSet.of( new FullQualifiedName( KryptnosticEdmProvider.NAMESPACE, product.getName() ) ) );

    }
}
