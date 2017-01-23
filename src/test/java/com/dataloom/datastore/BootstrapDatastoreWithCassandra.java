package com.dataloom.datastore;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.datastore.Datastore;
import com.dataloom.datastore.services.CassandraDataManager;
import com.dataloom.edm.exceptions.TypeExistsException;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.internal.Schema;
import com.dataloom.edm.schemas.manager.HazelcastSchemaManager;
import com.geekbeast.rhizome.tests.bootstrap.CassandraBootstrap;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.rhizome.pods.SparkPod;

public class BootstrapDatastoreWithCassandra extends CassandraBootstrap {
    public static final String               NAMESPACE                 = "testcsv";
    protected static EdmManager              dms;
    protected static AuthorizationManager    am;
    protected static CassandraDataManager    dataService;
    protected static HazelcastSchemaManager  schemaManager;

    protected static final Set<Class<?>>     PODS                      = Sets.newHashSet( SparkPod.class );
    protected static final Datastore         ds                        = new Datastore();
    protected static final Set<String>       PROFILES                  = Sets.newHashSet( "local", "cassandra" );
    protected static final String            SALARY                    = "salary";
    protected static final String            EMPLOYEE_NAME             = "employee_name";
    protected static final String            EMPLOYEE_TITLE            = "employee_title";
    protected static final String            EMPLOYEE_DEPT             = "employee_dept";
    protected static final String            EMPLOYEE_ID               = "employee_id";
    protected static final String            ENTITY_SET_NAME           = "Employees";

    protected static final String            PROPERTY_TYPE_EXISTS_MSG  = "Property Type of same name exists.";
    protected static final String            ENTITY_TYPE_EXISTS_MSG    = "Entity type of same name already exists.";
    protected static final String            ENTITY_SET_EXISTS_MSG     = "Entity set already exists.";
    protected static final String            SCHEMA_EXISTS_MSG         = "Failed to create schema.";

    protected static final String            ENTITY_TYPE_NAME          = "employee";
    protected static final FullQualifiedName ENTITY_TYPE               = new FullQualifiedName(
            NAMESPACE,
            ENTITY_TYPE_NAME );

    // created by Ho Chung to populate two more entity Types
    protected static final FullQualifiedName ENTITY_TYPE_MARS          = new FullQualifiedName(
            NAMESPACE,
            "employeeMars" );
    protected static final FullQualifiedName ENTITY_TYPE_SATURN        = new FullQualifiedName(
            NAMESPACE,
            "employeeSaturn" );
    protected static final String            SCHEMA_NAME               = "csv";

    protected static UUID                    EMPLOYEE_ID_PROP_ID       = UUID.randomUUID();
    protected static UUID                    EMPLOYEE_TITLE_PROP_ID    = UUID.randomUUID();
    protected static UUID                    EMPLOYEE_NAME_PROP_ID     = UUID.randomUUID();
    protected static UUID                    EMPLOYEE_DEPT_PROP_ID     = UUID.randomUUID();
    protected static UUID                    EMPLOYEE_SALARY_PROP_ID   = UUID.randomUUID();
    protected static UUID                    METADATA_LEVELS_ID        = UUID.randomUUID();
    protected static UUID                    METADATA_LEVELS_MARS_ID   = UUID.randomUUID();
    protected static UUID                    METADATA_LEVELS_SATURN_ID = UUID.randomUUID();

    protected static final PropertyType      EMPLOYEE_ID_PROP_TYPE     = new PropertyType(
            EMPLOYEE_ID_PROP_ID,
            new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
            "Employee ID",
            Optional
                    .of( "ID of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.Guid );

    protected static final PropertyType      EMPLOYEE_TITLE_PROP_TYPE  = new PropertyType(
            EMPLOYEE_TITLE_PROP_ID,
            new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ),
            "Title",
            Optional.of( "Title of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.String );

    protected static final PropertyType      EMPLOYEE_NAME_PROP_TYPE   = new PropertyType(
            EMPLOYEE_NAME_PROP_ID,
            new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ),
            "Name",
            Optional
                    .of( "Name of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.String );
    protected static final PropertyType      EMPLOYEE_DEPT_PROP_TYPE   = new PropertyType(
            EMPLOYEE_DEPT_PROP_ID,
            new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ),
            "Department",
            Optional
                    .of( "Department of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.String );
    protected static final PropertyType      EMPLOYEE_SALARY_PROP_TYPE = new PropertyType(
            EMPLOYEE_SALARY_PROP_ID,
            new FullQualifiedName( NAMESPACE, SALARY ),
            "Salary",
            Optional.of( "Salary of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.Int64 );

    protected static EntityType              METADATA_LEVELS;
    protected static EntityType              METADATA_LEVELS_SATURN;
    protected static EntityType              METADATA_LEVELS_MARS;
    protected static EntitySet               EMPLOYEES;

    protected static final ReadWriteLock     initLock                  = new ReentrantReadWriteLock();

    protected static final Principal         principal                 = new Principal(
            PrincipalType.USER,
            "tests|blahblah" );
    protected final Logger                   logger                    = LoggerFactory.getLogger( getClass() );

    static {
        if ( initLock.writeLock().tryLock() ) {
            ds.intercrop( PODS.toArray( new Class<?>[ 0 ] ) );
            try {
                ds.start( PROFILES.toArray( new String[ 0 ] ) );
            } catch ( Exception e ) {
                throw new IllegalStateException( "Unable to start datastore", e );
            }
            dms = ds.getContext().getBean( EdmManager.class );
            am = ds.getContext().getBean( AuthorizationManager.class );
            dataService = ds.getContext().getBean( CassandraDataManager.class );
            schemaManager = ds.getContext().getBean( HazelcastSchemaManager.class );
            setupDatamodel();
        }
    }

    private static void setupDatamodel() {
        createPropertyTypes();
        createEntityTypes();
        createEntitySets();

        schemaManager.createOrUpdateSchemas( new Schema(
                new FullQualifiedName( NAMESPACE, SCHEMA_NAME ),
                ImmutableSet.of( EMPLOYEE_ID_PROP_TYPE,
                        EMPLOYEE_TITLE_PROP_TYPE,
                        EMPLOYEE_NAME_PROP_TYPE,
                        EMPLOYEE_DEPT_PROP_TYPE,
                        EMPLOYEE_SALARY_PROP_TYPE ),
                ImmutableSet.of( METADATA_LEVELS, METADATA_LEVELS_MARS, METADATA_LEVELS_SATURN ) ) );

        Assert.assertTrue(
                dms.checkEntitySetExists( ENTITY_SET_NAME ) );
    }

    public static EntityType from( String modifier ) {
        UUID id;
        switch ( modifier ) {
            case "Saturn":
                id = METADATA_LEVELS_SATURN_ID;
                break;
            case "Mars":
                id = METADATA_LEVELS_MARS_ID;
                break;
            case "":
            default:
                id = METADATA_LEVELS_ID;
        }
        return new EntityType(
                id,
                new FullQualifiedName( NAMESPACE, ENTITY_TYPE_NAME + modifier ),
                modifier + " Employees",
                Optional.of( modifier + " Employees of the city of Chicago" ),
                ImmutableSet.of(),
                ImmutableSet.of( EMPLOYEE_ID_PROP_ID ),
                ImmutableSet.of( EMPLOYEE_ID_PROP_ID,
                        EMPLOYEE_TITLE_PROP_ID,
                        EMPLOYEE_NAME_PROP_ID,
                        EMPLOYEE_DEPT_PROP_ID,
                        EMPLOYEE_SALARY_PROP_ID ) );
    }

    private static void createPropertyTypes() {
        try {
            dms.createPropertyTypeIfNotExists( EMPLOYEE_ID_PROP_TYPE );
        } catch ( TypeExistsException e ) {
            EMPLOYEE_ID_PROP_ID = dms.getTypeAclKey( EMPLOYEE_ID_PROP_TYPE.getType() );
        }
        try {
            dms.createPropertyTypeIfNotExists( EMPLOYEE_TITLE_PROP_TYPE );
        } catch ( TypeExistsException e ) {
            EMPLOYEE_TITLE_PROP_ID = dms.getTypeAclKey( EMPLOYEE_TITLE_PROP_TYPE.getType() );
        }
        try {
            dms.createPropertyTypeIfNotExists( EMPLOYEE_NAME_PROP_TYPE );
        } catch ( TypeExistsException e ) {
            EMPLOYEE_NAME_PROP_ID = dms.getTypeAclKey( EMPLOYEE_NAME_PROP_TYPE.getType() );
        }
        try {
            dms.createPropertyTypeIfNotExists( EMPLOYEE_DEPT_PROP_TYPE );
        } catch ( TypeExistsException e ) {
            EMPLOYEE_DEPT_PROP_ID = dms.getTypeAclKey( EMPLOYEE_DEPT_PROP_TYPE.getType() );
        }
        try {
            dms.createPropertyTypeIfNotExists( EMPLOYEE_SALARY_PROP_TYPE );
        } catch ( TypeExistsException e ) {
            EMPLOYEE_SALARY_PROP_ID = dms.getTypeAclKey( EMPLOYEE_SALARY_PROP_TYPE.getType() );
        }
    }

    private static void createEntityTypes() {
        METADATA_LEVELS = from( "" );
        METADATA_LEVELS_SATURN = from( "Saturn" );
        METADATA_LEVELS_MARS = from( "Mars" );

        try {
            dms.createEntityType( METADATA_LEVELS );
        } catch ( TypeExistsException e ) {
            METADATA_LEVELS_ID = dms.getTypeAclKey( METADATA_LEVELS.getType() );
        }
        try {
            dms.createEntityType( METADATA_LEVELS_SATURN );
        } catch ( TypeExistsException e ) {
            METADATA_LEVELS_MARS_ID = dms.getTypeAclKey( METADATA_LEVELS_MARS.getType() );
        }
        try {
            dms.createEntityType( METADATA_LEVELS_MARS );
        } catch ( TypeExistsException e ) {
            METADATA_LEVELS_SATURN_ID = dms.getTypeAclKey( METADATA_LEVELS_SATURN.getType() );
        }
    }

    private static void createEntitySets() {
        EMPLOYEES = new EntitySet(
                METADATA_LEVELS_ID,
                ENTITY_SET_NAME,
                ENTITY_SET_NAME,
                Optional.of( "Names and salaries of Chicago employees" ) );

        try {
            dms.createEntitySet(
                    principal,
                    EMPLOYEES );
        } catch ( TypeExistsException e ) {
            // Only acceptable exception is Entity Set already exists.
            Assert.assertEquals( ENTITY_SET_EXISTS_MSG, e.getMessage() );
        }
    }
}
