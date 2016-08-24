package com.kryptnostic.datastore.edm;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.geekbeast.rhizome.tests.pods.CassandraTestPod;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.types.services.EdmManager;

public class DatastoreTests {
    private static final DatastoreServices ds        = new DatastoreServices();
    public static final String             NAMESPACE = "tests";

    @BeforeClass
    public static void init() {
        // This is fine since unless cassandra is specified as runtime argument production cassandra pod won't be
        // activated
        CassandraTestPod.startCassandra();
        ds.sprout( CassandraTestPod.PROFILE );
    }

    @Test
    public void testkeeEntityType() {
        EdmManager dms = ds.getContext().getBean( EdmManager.class );

        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( "aclId" )
                .setDatatype( EdmPrimitiveTypeKind.Guid ).setMultiplicity( 0 ) );
        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( "type" )
                .setDatatype( EdmPrimitiveTypeKind.Guid ).setMultiplicity( 0 ) );
        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( "clock" )
                .setDatatype( EdmPrimitiveTypeKind.Guid ).setMultiplicity( 0 ) );
        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( "objectId" )
                .setDatatype( EdmPrimitiveTypeKind.Int64 ).setMultiplicity( 0 ) );
        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( "version" )
                .setDatatype( EdmPrimitiveTypeKind.Int64 ).setMultiplicity( 0 ) );

        EntityType metadataLevel = new EntityType().setNamespace( NAMESPACE ).setName( "metadataLevel" )
                .setKey( ImmutableSet.of( new FullQualifiedName( NAMESPACE, "aclId" ) ) )
                .setProperties( ImmutableSet.of( new FullQualifiedName( NAMESPACE, "aclId" ),
                        new FullQualifiedName( NAMESPACE, "type" ),
                        new FullQualifiedName( NAMESPACE, "clock" ),
                        new FullQualifiedName( NAMESPACE, "objectId" ),
                        new FullQualifiedName( NAMESPACE, "version" ) ) );
        
        dms.createEntityType( metadataLevel );
        dms.createEntitySet( new FullQualifiedName( NAMESPACE, "metadataLevel" ),
                "metadataLevels",
                "The entity set title" );

        dms.createSchema( NAMESPACE,
                "anubis",
                ACLs.EVERYONE_ACL,
                ImmutableSet.of( new FullQualifiedName( NAMESPACE, metadataLevel.getName() ) ) );

        Assert.assertTrue(
                dms.isExistingEntitySet( new FullQualifiedName( NAMESPACE, "metadataLevel" ), "metadataLevels" ) );
    }

}
