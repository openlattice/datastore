package com.kryptnostic.datastore.edm;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.types.services.EdmManager;

public class BootstrapDatastoreWithCassandra {
    public static final String               NAMESPACE       = "testcsv";
    protected static final DatastoreServices ds              = new DatastoreServices();
    protected static final String            SALARY          = "salary";
    protected static final String            EMPLOYEE_NAME   = "employee-name";
    protected static final String            EMPLOYEE_TITLE  = "employee-title";
    protected static final String            EMPLOYEE_DEPT   = "employee-dept";
    protected static final String            EMPLOYEE_ID     = "employee-id";
    protected static final String            ENTITY_SET_NAME = "Employees";
    protected static final FullQualifiedName ENTITY_TYPE     = new FullQualifiedName( NAMESPACE, "employee" );

    @BeforeClass
    public static void init() {
        ds.sprout("cassandra");
        setupDatamodel();
    }

    private static void setupDatamodel() {
        EdmManager dms = ds.getContext().getBean( EdmManager.class );

        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( EMPLOYEE_ID )
                .setDatatype( EdmPrimitiveTypeKind.Guid ).setMultiplicity( 0 ) );
        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( EMPLOYEE_TITLE )
                .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 ) );
        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( EMPLOYEE_NAME )
                .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 ) );
        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( EMPLOYEE_DEPT )
                .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 ) );
        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setName( SALARY )
                .setDatatype( EdmPrimitiveTypeKind.Int64 ).setMultiplicity( 0 ) );

        EntityType metadataLevel = new EntityType().setNamespace( NAMESPACE ).setName( ENTITY_TYPE.getName() )
                .setKey( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ) ) )
                .setProperties( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ),
                        new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ),
                        new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ),
                        new FullQualifiedName( NAMESPACE, SALARY ) ) );

        dms.createEntityType( metadataLevel );
        dms.createEntitySet( ENTITY_TYPE,
                ENTITY_SET_NAME,
                "The entity set title" );
        dms.ge
        dms.createSchema( NAMESPACE,
                "csv",
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
