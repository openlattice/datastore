package com.kryptnostic.datastore.edm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;
import java.io.OutputStream;
import java.util.UUID;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmEntityContainer;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.core.edm.EdmEntityContainerImpl;
import org.apache.olingo.commons.core.edm.EdmEntitySetImpl;
import org.apache.olingo.commons.core.edm.EdmEntityTypeImpl;
import org.apache.olingo.commons.core.edm.EdmProviderImpl;
import org.apache.olingo.server.api.ODataApplicationException;
import org.junit.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.HttpMessageNotWritableException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.Employee;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.UUIDs.Syncs;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.datastore.odata.KryptnosticEdmProvider;
import com.kryptnostic.datastore.odata.Transformers;
import com.kryptnostic.datastore.odata.Transformers.EntityTypeTransformer;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.ODataStorageService;
import com.kryptnostic.datastore.converters.IterableCsvHttpMessageConverter;
import com.kryptnostic.datastore.services.EdmService;

public class DatastoreTests extends BootstrapDatastoreWithCassandra {

    private static final Multimap<String, Object> m = HashMultimap.create();

    @Test
    public void testSerialization() throws HttpMessageNotWritableException, IOException {
        IterableCsvHttpMessageConverter converter = new IterableCsvHttpMessageConverter(
                ds.getContext().getBean( EdmService.class ) );
        m.put( new FullQualifiedName( NAMESPACE, SALARY ).getFullQualifiedNameAsString(), 1 );
        m.put( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ).getFullQualifiedNameAsString(), UUID.randomUUID() );
        m.put( new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ).getFullQualifiedNameAsString(), "Master Chief" );
        converter.write( ImmutableList.of( m ), null, null, new HttpOutputMessage() {

            @Override
            public HttpHeaders getHeaders() {
                return new HttpHeaders();
            }

            @Override
            public OutputStream getBody() throws IOException {
                return System.out;
            }
        } );
    }

    @Test
    public void testCreateEntityType() {
        ODataStorageService esc = ds.getContext().getBean( ODataStorageService.class );
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

        // esc.readEntityData( edmEntitySet, keyParams );
    }

    @Test
    public void polulateEmployeeCsv() throws IOException {
        ODataStorageService esc = ds.getContext().getBean( ODataStorageService.class );
        Property employeeId;
        Property employeeName;
        Property employeeTitle;
        Property employeeDept;
        Property employeeSalary;

        try ( FileReader fr = new FileReader( "src/test/resources/employees.csv" );
                BufferedReader br = new BufferedReader( fr ) ) {

            String line;
            while ( ( line = br.readLine() ) != null ) {
                Employee employee = Employee.EmployeeCsvReader.getEmployee( line );
                System.out.println( employee.toString() );

                employeeId = new Property();
                employeeId.setName( EMPLOYEE_ID );
                employeeId.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ).getFullQualifiedNameAsString() );
                employeeId.setValue( ValueType.PRIMITIVE, UUID.randomUUID() );

                employeeName = new Property();
                employeeName.setName( EMPLOYEE_NAME );
                employeeName
                        .setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ).getFullQualifiedNameAsString() );
                employeeName.setValue( ValueType.PRIMITIVE, employee.getName() );

                employeeTitle = new Property();
                employeeTitle.setName( EMPLOYEE_TITLE );
                employeeTitle
                        .setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ).getFullQualifiedNameAsString() );
                employeeTitle.setValue( ValueType.PRIMITIVE, employee.getTitle() );

                employeeDept = new Property();
                employeeDept.setName( EMPLOYEE_DEPT );
                employeeDept
                        .setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ).getFullQualifiedNameAsString() );
                employeeDept.setValue( ValueType.PRIMITIVE, employee.getDept() );

                employeeSalary = new Property();
                employeeSalary.setName( SALARY );
                employeeSalary.setType( new FullQualifiedName( NAMESPACE, SALARY ).getFullQualifiedNameAsString() );
                employeeSalary.setValue( ValueType.PRIMITIVE, (long) employee.getSalary() );

                Entity entity = new Entity();
                entity.setType( ENTITY_TYPE.getFullQualifiedNameAsString() );
                entity.addProperty( employeeId )
                        .addProperty( employeeName )
                        .addProperty( employeeTitle )
                        .addProperty( employeeDept )
                        .addProperty( employeeSalary );

                esc.createEntityData( ACLs.EVERYONE_ACL,
                        Syncs.BASE.getSyncId(),
                        ENTITY_SET_NAME,
                        ENTITY_TYPE,
                        entity );
                
                //Created by Ho Chung for testing different entity types
                //add entityType "employeeMars"
                esc.createEntityData( ACLs.EVERYONE_ACL,
                        Syncs.BASE.getSyncId(),
                        ENTITY_SET_NAME,
                        ENTITY_TYPE_MARS,
                        entity );
              //add entityType "employeeSaturn"
                esc.createEntityData( ACLs.EVERYONE_ACL,
                        Syncs.BASE.getSyncId(),
                        ENTITY_SET_NAME,
                        ENTITY_TYPE_SATURN,
                        entity );

            }
        }
    }

    @Test
    public void testRead() {
    	Set<FullQualifiedName> properties = ImmutableSet.of( 
    			new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
        		new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ),
        		new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ),
        		new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ),
        		new FullQualifiedName( NAMESPACE, SALARY ) );
    	
    	ODataStorageService esc = ds.getContext().getBean( ODataStorageService.class );
    	EdmManager dms = ds.getContext().getBean( EdmManager.class );
        KryptnosticEdmProvider provider = new KryptnosticEdmProvider( dms );
        Edm edm = new EdmProviderImpl( provider );

        CsdlEntityContainerInfo info = new CsdlEntityContainerInfo().setContainerName( ENTITY_TYPE );
        EdmEntityContainer edmEntityContainer = new EdmEntityContainerImpl( edm, provider, info );

        CsdlEntityType csdlEntityType = new EntityTypeTransformer( dms ).transform( 
        		new EntityType()
        		.setName( ENTITY_TYPE.getName() )
        		.setNamespace( ENTITY_TYPE.getNamespace() )
        		.setProperties( properties )
        		.setKey( ImmutableSet.of() ) );
        EdmEntityType edmEntityType = new EdmEntityTypeImpl( edm, ENTITY_TYPE, csdlEntityType );

        CsdlEntitySet csdlEntitySet = Transformers.transform( new EntitySet().setName( ENTITY_SET_NAME ).setTitle( ENTITY_SET_NAME ).setType( ENTITY_TYPE) );
        EdmEntitySet edmEntitySet = new EdmEntitySetImpl( edm, edmEntityContainer, csdlEntitySet );

        try {
			EntityCollection ec = esc.readEntitySetData( edmEntitySet );
			ec.forEach( currEntity -> System.out.println( currEntity ) );
		} catch (ODataApplicationException e) {
			e.printStackTrace();
		}
    }
}
