package com.kryptnostic.datastore;

import digital.loom.rhizome.authentication.Auth0Pod;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.odata.KryptnosticEdmProvider;
import com.kryptnostic.datastore.pods.DataStoreSecurityPod;
import com.kryptnostic.datastore.pods.DatastoreServicesPod;
import com.kryptnostic.datastore.pods.DatastoreServletsPod;
import com.kryptnostic.datastore.pods.DatastoreStreamSerializersPod;
import com.kryptnostic.datastore.pods.DatastoreTypeCodecsPod;
import com.kryptnostic.datastore.serialization.FullQualifedNameJacksonDeserializer;
import com.kryptnostic.datastore.serialization.FullQualifedNameJacksonSerializer;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.mapstores.pods.BaseSerializersPod;
import com.kryptnostic.rhizome.configuration.websockets.BaseRhizomeServer;
import com.kryptnostic.rhizome.core.RhizomeApplicationServer;
import com.kryptnostic.rhizome.hazelcast.serializers.RhizomeUtils.Pods;
import com.kryptnostic.rhizome.pods.CassandraPod;
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod;
import com.kryptnostic.rhizome.registries.ObjectMapperRegistry;

public class Datastore extends BaseRhizomeServer {
    @Deprecated
    public static final String ES_PRODUCTS_NAME = "Products";
    public static final Class<?>[] webPods       = new Class<?>[] { DatastoreServletsPod.class,
            DataStoreSecurityPod.class, };
    public static final Class<?>[] rhizomePods   = new Class<?>[] {
            CassandraPod.class,
            BaseSerializersPod.class,
            RegistryBasedHazelcastInstanceConfigurationPod.class,
            Auth0Pod.class};
            
    public static final Class<?>[] datastorePods = new Class<?>[] {
            DatastoreServicesPod.class,
            DatastoreTypeCodecsPod.class, DatastoreStreamSerializersPod.class
    };

    static {
        ObjectMapperRegistry.foreach( FullQualifedNameJacksonSerializer::registerWithMapper );
        ObjectMapperRegistry.foreach( FullQualifedNameJacksonDeserializer::registerWithMapper );
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

        final String ET_PRODUCT_NAME = "Product";
        final FullQualifiedName ET_PRODUCT_FQN = new FullQualifiedName(
                KryptnosticEdmProvider.NAMESPACE,
                ET_PRODUCT_NAME );


        dms.createPropertyType( new PropertyType().setNamespace( KryptnosticEdmProvider.NAMESPACE ).setName( "ID" )
                .setDatatype( EdmPrimitiveTypeKind.Int32 ).setMultiplicity( 0 ) );
        dms.createPropertyType( new PropertyType().setNamespace( KryptnosticEdmProvider.NAMESPACE ).setName( "Name" )
                .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 ) );
        dms.createPropertyType(
                new PropertyType().setNamespace( KryptnosticEdmProvider.NAMESPACE ).setName( "Description" )
                        .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 ) );
        EntityType product = new EntityType()
                .setNamespace( KryptnosticEdmProvider.NAMESPACE )
                .setName( ET_PRODUCT_NAME )
                .setKey( ImmutableSet.of( new FullQualifiedName( KryptnosticEdmProvider.NAMESPACE, "ID" ) ) )
                .setProperties( ImmutableSet.of( new FullQualifiedName( KryptnosticEdmProvider.NAMESPACE, "ID" ),
                        new FullQualifiedName( KryptnosticEdmProvider.NAMESPACE, "Name" ),
                        new FullQualifiedName( KryptnosticEdmProvider.NAMESPACE, "Description" ) ) );

        dms.createEntityType( product );
        dms.createEntitySet( ET_PRODUCT_FQN, ES_PRODUCTS_NAME, null );
        dms.createEntitySet( new FullQualifiedName( KryptnosticEdmProvider.NAMESPACE, "metadataLevel" ),
                "metadataLevels",
                null );

        dms.createSchema( KryptnosticEdmProvider.NAMESPACE,
                "agora",
                ACLs.EVERYONE_ACL,
                ImmutableSet.of( new FullQualifiedName( KryptnosticEdmProvider.NAMESPACE, product.getName() ) ) );
    }
}
