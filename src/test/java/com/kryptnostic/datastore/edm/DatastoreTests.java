package com.kryptnostic.datastore.edm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.UUID;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Test;

import com.kryptnostic.conductor.rpc.Employee;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.UUIDs.Syncs;
import com.kryptnostic.datastore.services.EntityStorageClient;

public class DatastoreTests extends BootstrapDatastoreWithCassandra {

    //@Test
    public void testCreateEntityType() {
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

        //        esc.readEntityData( edmEntitySet, keyParams );
    }

    @Test
    public void polulateEmployeeCsv() throws IOException {
        EntityStorageClient esc = ds.getContext().getBean( EntityStorageClient.class );
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
                esc.createEntityData( ACLs.EVERYONE_ACL,
                        Syncs.BASE.getSyncId(),
                        ENTITY_SET_NAME,
                        ENTITY_TYPE_MARS,
                        entity );
                
                esc.createEntityData( ACLs.EVERYONE_ACL,
                        Syncs.BASE.getSyncId(),
                        ENTITY_SET_NAME,
                        ENTITY_TYPE_SATURN,
                        entity );

            }
        }

    }
}
