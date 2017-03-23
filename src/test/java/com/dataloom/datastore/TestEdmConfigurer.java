/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.datastore;

import java.util.Arrays;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Assert;

import com.dataloom.authorization.Principal;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.Schema;
import com.dataloom.edm.exceptions.TypeExistsException;
import com.dataloom.edm.schemas.manager.HazelcastSchemaManager;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.kryptnostic.datastore.services.EdmManager;

/**
 * @author Ho Chung Siu &lt;hochung@kryptnostic.com&gt;
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class TestEdmConfigurer {
    public static final String            NAMESPACE                 = "testcsv";
    public static final String            SCHEMA_NAME               = "csv";
    public static final String            SALARY                    = "salary";
    public static final String            EMPLOYEE_NAME             = "employee_name";
    public static final String            EMPLOYEE_TITLE            = "employee_title";
    public static final String            EMPLOYEE_DEPT             = "employee_dept";
    public static final String            EMPLOYEE_ID               = "employee_id";
    public static final String            ENTITY_SET_NAME           = "Employees";
    public static final String            ENTITY_TYPE_NAME          = "employee";

    public static final FullQualifiedName ENTITY_TYPE               = new FullQualifiedName(
            NAMESPACE,
            ENTITY_TYPE_NAME );

    public static UUID                    METADATA_LEVELS_ID        = UUID.randomUUID();
    public static UUID                    METADATA_LEVELS_MARS_ID   = UUID.randomUUID();
    public static UUID                    METADATA_LEVELS_SATURN_ID = UUID.randomUUID();
    public static UUID                    EMPLOYEE_ID_PROP_ID       = UUID.randomUUID();

    protected static final PropertyType   EMPLOYEE_ID_PROP_TYPE     = new PropertyType(
            EMPLOYEE_ID_PROP_ID,
            new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
            "Employee ID",
            Optional
                    .of( "ID of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.Guid );
    public static EntityType              METADATA_LEVELS;
    public static EntityType              METADATA_LEVELS_SATURN;
    public static EntityType              METADATA_LEVELS_MARS;
    public static EntitySet               EMPLOYEES;
    public static UUID                    EMPLOYEE_TITLE_PROP_ID    = UUID.randomUUID();
    public static final PropertyType      EMPLOYEE_TITLE_PROP_TYPE  = new PropertyType(
            EMPLOYEE_TITLE_PROP_ID,
            new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ),
            "Title",
            Optional.of( "Title of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.String );
    public static UUID                    EMPLOYEE_NAME_PROP_ID     = UUID.randomUUID();
    protected static final PropertyType   EMPLOYEE_NAME_PROP_TYPE   = new PropertyType(
            EMPLOYEE_NAME_PROP_ID,
            new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ),
            "Name",
            Optional
                    .of( "Name of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.String );
    public static UUID                    EMPLOYEE_DEPT_PROP_ID     = UUID.randomUUID();
    protected static final PropertyType   EMPLOYEE_DEPT_PROP_TYPE   = new PropertyType(
            EMPLOYEE_DEPT_PROP_ID,
            new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ),
            "Department",
            Optional
                    .of( "Department of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.String );
    public static UUID                    EMPLOYEE_SALARY_PROP_ID   = UUID.randomUUID();
    protected static final PropertyType   EMPLOYEE_SALARY_PROP_TYPE = new PropertyType(
            EMPLOYEE_SALARY_PROP_ID,
            new FullQualifiedName( NAMESPACE, SALARY ),
            "Salary",
            Optional.of( "Salary of an employee of the city of Chicago." ),
            ImmutableSet.of(),
            EdmPrimitiveTypeKind.Int64 );

    public static final FullQualifiedName ENTITY_TYPE_MARS          = new FullQualifiedName(
            NAMESPACE,
            "employeeMars" );
    public static final FullQualifiedName ENTITY_TYPE_SATURN        = new FullQualifiedName(
            NAMESPACE,
            "employeeSaturn" );

    static void setupDatamodel( Principal principal, EdmManager dms, HazelcastSchemaManager schemaManager ) {
        createPropertyTypes( dms );
        createEntityTypes( dms );
        createEntitySets( dms, principal );

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
                Sets.newLinkedHashSet( Arrays.asList( EMPLOYEE_ID_PROP_ID ) ),
                Sets.newLinkedHashSet( Arrays.asList( EMPLOYEE_ID_PROP_ID,
                        EMPLOYEE_TITLE_PROP_ID,
                        EMPLOYEE_NAME_PROP_ID,
                        EMPLOYEE_DEPT_PROP_ID,
                        EMPLOYEE_SALARY_PROP_ID ) ),
                Optional.absent(),
                SecurableObjectType.EntityType );
    }

    private static void createPropertyTypes( EdmManager dms ) {
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

    private static void createEntityTypes( EdmManager dms ) {
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

    private static void createEntitySets( EdmManager dms, Principal principal ) {
        EMPLOYEES = dms.getEntitySet( ENTITY_SET_NAME );
        if ( EMPLOYEES == null ) {
            EMPLOYEES = new EntitySet(
                    METADATA_LEVELS_ID,
                    ENTITY_SET_NAME,
                    ENTITY_SET_NAME,
                    Optional.of( "Names and salaries of Chicago employees" ),
                    ImmutableSet.of( "support@kryptnostic.com" ) );
            dms.createEntitySet(
                    principal,
                    EMPLOYEES );
        }
    }
}
