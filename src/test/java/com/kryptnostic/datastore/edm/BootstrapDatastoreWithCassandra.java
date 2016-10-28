package com.kryptnostic.datastore.edm;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.Constants;
import com.kryptnostic.datastore.services.DataService;
import com.kryptnostic.datastore.services.EdmDetailsAdapter;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.PermissionsService;

public class BootstrapDatastoreWithCassandra {
    public static final String               NAMESPACE       = "testcsv";
    protected static final DatastoreServices ds              = new DatastoreServices();
    protected static EdmManager              dms;
    protected static PermissionsService      ps;
    protected static DataService             dataService;
    protected static final String            SALARY          = "salary";
    protected static final String            EMPLOYEE_NAME   = "employee-name";
    protected static final String            EMPLOYEE_TITLE  = "employee-title";
    protected static final String            EMPLOYEE_DEPT   = "employee-dept";
    protected static final String            EMPLOYEE_ID     = "employee-id";
    protected static final String            ENTITY_SET_NAME = "Employees";
    protected static final FullQualifiedName ENTITY_TYPE     = new FullQualifiedName( NAMESPACE, "employee" );
    //created by Ho Chung to populate two more entity Types
    protected static final FullQualifiedName ENTITY_TYPE_MARS= new FullQualifiedName( NAMESPACE, "employeeMars" );
    protected static final FullQualifiedName ENTITY_TYPE_SATURN= new FullQualifiedName( NAMESPACE, "employeeSaturn" );
    protected static final String            SCHEMA_NAME     = "csv";

    /**
     * WARNING By Ho Chung
     * The God role is the super user that can manage all access rights
     */
    protected static final User              USER_GOD = new User( "God", Sets.newHashSet(Constants.ROLE_ADMIN) );
	
	protected static class User{
	    private String name;
	    private Set<String> roles;
        
	    public User( String name, Set<String> roles ){
	        this.name = name;
	        this.roles = roles;
	    } 
	    
	    public Set<String> getRoles() {
            return roles;
        }
        public void setRoles( Set<String> roles ) {
            this.roles = roles;
        }
        public String getName() {
            return name;
        }
	    
        public void addRoles( Set<String> rolesToAdd ){
            this.roles.addAll( rolesToAdd );
        }

        public void removeRoles( Set<String> rolesToRemove ){
            this.roles.removeAll( rolesToRemove );
        }
	}
	
    @BeforeClass
    public static void init() {
        ds.sprout("cassandra");
        dms = ds.getContext().getBean( EdmManager.class );
        ps = ds.getContext().getBean( PermissionsService.class );
        dataService = ds.getContext().getBean( DataService.class );
        
        //You are God right now - this would be the superUser that has rights to do everything to the types created
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

        EntityType metadataLevelSaturn = new EntityType().setNamespace( NAMESPACE ).setName( ENTITY_TYPE_SATURN.getName() )
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
    
	protected static void setUser( User user ){
		if ( user != null ){
		    dms.setCurrentUserForDebug( user.getName(), user.getRoles() );
		    EdmDetailsAdapter.setCurrentUserForDebug( user.getName(), user.getRoles() );
		    try {
                dataService.setCurrentUserForDebug( user.getName(), user.getRoles() );
            } catch ( InterruptedException e ) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch ( ExecutionException e ) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
		}
	}
}
