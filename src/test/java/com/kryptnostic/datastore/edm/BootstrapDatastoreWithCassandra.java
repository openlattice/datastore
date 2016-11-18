package com.kryptnostic.datastore.edm;

import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.AfterClass;
import org.junit.Assert;

import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.datastore.services.ActionAuthorizationService;
import com.kryptnostic.datastore.services.DataService;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.PermissionsService;
import com.kryptnostic.rhizome.pods.SparkPod;

public class BootstrapDatastoreWithCassandra {
    private static HazelcastInstance            hazelcast;

    public static final String                  NAMESPACE          = "testcsv";
    protected static EdmManager                 dms;
    protected static PermissionsService         ps;
    protected static DataService                dataService;
    protected static ActionAuthorizationService authzService;

    protected static final Set<Class<?>>        PODS               = Sets.newHashSet( SparkPod.class );
    protected static final DatastoreServices    ds                 = new DatastoreServices();
    protected static final Set<String>          PROFILES           = Sets.newHashSet( "local", "cassandra" );
    protected static final String               SALARY             = "salary";
    protected static final String               EMPLOYEE_NAME      = "employee_name";
    protected static final String               EMPLOYEE_TITLE     = "employee_title";
    protected static final String               EMPLOYEE_DEPT      = "employee_dept";
    protected static final String               EMPLOYEE_ID        = "employee_id";
    protected static final String               ENTITY_SET_NAME    = "Employees";
    protected static final FullQualifiedName    ENTITY_TYPE        = new FullQualifiedName( NAMESPACE, "employee" );
    // created by Ho Chung to populate two more entity Types
    protected static final FullQualifiedName    ENTITY_TYPE_MARS   = new FullQualifiedName( NAMESPACE, "employeeMars" );
    protected static final FullQualifiedName    ENTITY_TYPE_SATURN = new FullQualifiedName(
            NAMESPACE,
            "employeeSaturn" );
    protected static final String               SCHEMA_NAME        = "csv";
    protected static final Semaphore            initLock           = new Semaphore( 1 );

    public static void init() {
        if ( initLock.tryAcquire() ) {
            ds.intercrop( PODS.toArray( new Class<?>[ 0 ] ) );
            ds.sprout( PROFILES.toArray( new String[ 0 ] ) );
            dms = ds.getContext().getBean( EdmManager.class );
            ps = ds.getContext().getBean( PermissionsService.class );
            dataService = ds.getContext().getBean( DataService.class );
            authzService = ds.getContext().getBean( ActionAuthorizationService.class );

            setupDatamodel();
        }
    }

    private static void setupDatamodel() {

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

        EntityType metadataLevelMars = new EntityType().setNamespace( NAMESPACE ).setName( ENTITY_TYPE_MARS.getName() )
                .setKey( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ) ) )
                .setProperties( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ),
                        new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ),
                        new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ),
                        new FullQualifiedName( NAMESPACE, SALARY ) ) );

        dms.createEntityType( metadataLevelMars );

        EntityType metadataLevelSaturn = new EntityType().setNamespace( NAMESPACE )
                .setName( ENTITY_TYPE_SATURN.getName() )
                .setKey( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ) ) )
                .setProperties( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ),
                        new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ),
                        new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ),
                        new FullQualifiedName( NAMESPACE, SALARY ) ) );

        dms.createEntityType( metadataLevelSaturn );

        dms.createEntitySet( ENTITY_TYPE,
                ENTITY_SET_NAME,
                "The entity set title" );

        dms.createSchema( NAMESPACE,
                SCHEMA_NAME,
                ACLs.EVERYONE_ACL,
                ImmutableSet.of( ENTITY_TYPE, ENTITY_TYPE_MARS, ENTITY_TYPE_SATURN ) );

        Assert.assertTrue(
                dms.checkEntitySetExists( ENTITY_SET_NAME ) );
    }

//    @AfterClass
//    public static void shutdown() {
//        ds.plowUnder();
//    }

}
