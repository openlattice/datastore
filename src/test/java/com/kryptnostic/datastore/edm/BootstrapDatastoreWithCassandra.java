package com.kryptnostic.datastore.edm;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.rpc.ConductorCall;
import com.kryptnostic.conductor.rpc.Lambdas;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.Constants;
import com.kryptnostic.datastore.services.ActionAuthorizationService;
import com.kryptnostic.datastore.services.DataService;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.PermissionsService;

public class BootstrapDatastoreWithCassandra {

    @Inject
    private static HazelcastInstance            hazelcast;

    public static final String                  NAMESPACE          = "testcsv";
    protected static final DatastoreServices    ds                 = new DatastoreServices();
    protected static EdmManager                 dms;
    protected static PermissionsService         ps;
    protected static DataService                dataService;
    protected static ActionAuthorizationService authzService;
    protected static final String               SALARY             = "salary";
    protected static final String               EMPLOYEE_NAME      = "employee-name";
    protected static final String               EMPLOYEE_TITLE     = "employee-title";
    protected static final String               EMPLOYEE_DEPT      = "employee-dept";
    protected static final String               EMPLOYEE_ID        = "employee-id";
    protected static final String               ENTITY_SET_NAME    = "Employees";
    protected static final FullQualifiedName    ENTITY_TYPE        = new FullQualifiedName( NAMESPACE, "employee" );
    // created by Ho Chung to populate two more entity Types
    protected static final FullQualifiedName    ENTITY_TYPE_MARS   = new FullQualifiedName( NAMESPACE, "employeeMars" );
    protected static final FullQualifiedName    ENTITY_TYPE_SATURN = new FullQualifiedName(
            NAMESPACE,
            "employeeSaturn" );
    protected static final String               SCHEMA_NAME        = "csv";

    /**
     * WARNING By Ho Chung The God role is the super user that can manage all access rights
     */
    protected static final User                 USER_GOD           = new User(
            "God",
            Arrays.asList( Constants.ROLE_ADMIN ) );

    protected static class User {
        private String       name;
        private List<String> roles;

        public User( String name, List<String> roles ) {
            this.name = name;
            this.roles = roles;
        }

        public List<String> getRoles() {
            return roles;
        }

        public void setRoles( List<String> roles ) {
            this.roles = roles;
        }

        public String getName() {
            return name;
        }

        public void addRoles( List<String> rolesToAdd ) {
            this.roles.addAll( rolesToAdd );
        }

        public void removeRoles( List<String> rolesToRemove ) {
            this.roles.removeAll( rolesToRemove );
        }
    }

    @BeforeClass
    public static void init() {
        ds.sprout( "cassandra" );
        dms = ds.getContext().getBean( EdmManager.class );
        ps = ds.getContext().getBean( PermissionsService.class );
        dataService = ds.getContext().getBean( DataService.class );
        authzService = ds.getContext().getBean( ActionAuthorizationService.class );

        // You are God right now - this would be the superUser that has rights to do everything to the types created
        setUser( USER_GOD );
        setupDatamodel();
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
                dms.isExistingEntitySet( ENTITY_TYPE, ENTITY_SET_NAME ) );
    }

    @AfterClass
    public static void shutdown() {
        ds.plowUnder();
    }

    protected static void setUser( User user ) {
        /**
         * Check out feature/acl-local branch for a proper setUser function
         * need to modify a few pods ad-hocly to get conductor's user set as well - this is removed in the final version.
         */
    }
}
