package com.kryptnostic.datastore.edm;

import java.util.UUID;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.geekbeast.rhizome.tests.pods.CassandraTestPod;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.UUIDs.Syncs;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.types.services.EdmManager;
import com.kryptnostic.types.services.EntityStorageClient;

public class DatastoreTests {
    private static final String            SALARY          = "salary";
    private static final String            EMPLOYEE_NAME   = "employee-name";
    private static final String            EMPLOYEE_TITLE  = "employee-title";
    private static final String            EMPLOYEE_ID     = "employee-id";
    private static final DatastoreServices ds              = new DatastoreServices();
    public static final String             NAMESPACE       = "tests";
    private static final String            ENTITY_SET_NAME = "Employees";
    private static final FullQualifiedName ENTITY_TYPE     = new FullQualifiedName( NAMESPACE, "employee" );

    @BeforeClass
    public static void init() {
        // This is fine since unless cassandra is specified as runtime argument production cassandra pod won't be
        // activated
        CassandraTestPod.startCassandra();
        ds.sprout( CassandraTestPod.PROFILE );
        // ds.sprout( "cassandra" );
        setupDatamodel();
    }

    @Test
    public void testkeeEntityType() {
        EntityStorageClient esc = ds.getContext().getBean( EntityStorageClient.class );
        Property empId = new Property();
        Property empName = new Property();
        Property empTitle = new Property();
        Property empSalary = new Property();
        empId.setName( EMPLOYEE_ID );
        empId.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ).getFullQualifiedNameAsString() );
        empId.setValue( ValueType.PRIMITIVE, UUID.randomUUID() );

        empName.setName( EMPLOYEE_NAME );
        empName.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ).getFullQualifiedNameAsString() );
        empName.setValue( ValueType.PRIMITIVE, "Kung Fury" );

        empTitle.setName( EMPLOYEE_TITLE );
        empTitle.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ).getFullQualifiedNameAsString() );
        empTitle.setValue( ValueType.PRIMITIVE, "Kung Fu Master" );

        empSalary.setName( SALARY );
        empSalary.setType( new FullQualifiedName( NAMESPACE, SALARY ).getFullQualifiedNameAsString() );
        empSalary.setValue( ValueType.PRIMITIVE, Long.MAX_VALUE );

        Entity e = new Entity();
        e.setType( ENTITY_TYPE.getFullQualifiedNameAsString() );
        e.addProperty( empId ).addProperty( empName ).addProperty( empTitle ).addProperty( empSalary );
        esc.createEntityData( ACLs.EVERYONE_ACL,
                Syncs.BASE.getSyncId(),
                ENTITY_SET_NAME,
                ENTITY_TYPE,
                e );

    }

    private static void setupDatamodel() {
        EdmManager dms = ds.getContext().getBean( EdmManager.class );

        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( EMPLOYEE_ID )
                .setDatatype( EdmPrimitiveTypeKind.Guid ).setMultiplicity( 0 ) );
        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( EMPLOYEE_TITLE )
                .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 ) );
        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( EMPLOYEE_NAME )
                .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 ) );
        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( SALARY )
                .setDatatype( EdmPrimitiveTypeKind.Int64 ).setMultiplicity( 0 ) );

        EntityType metadataLevel = new EntityType().setNamespace( NAMESPACE ).setName( ENTITY_TYPE.getName() )
                .setKey( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ) ) )
                .setProperties( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ),
                        new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ),
                        new FullQualifiedName( NAMESPACE, SALARY ) ) );

        dms.createEntityType( metadataLevel );
        dms.createEntitySet( ENTITY_TYPE,
                ENTITY_SET_NAME,
                "The entity set title" );

        dms.createSchema( NAMESPACE,
                "anubis",
                ACLs.EVERYONE_ACL,
                ImmutableSet.of( ENTITY_TYPE ) );

        Assert.assertTrue(
                dms.isExistingEntitySet( ENTITY_TYPE, ENTITY_SET_NAME ) );

    }

    @AfterClass
    public static void shutdown() {
        ds.plowUnder();
    }
}
