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

package com.dataloom.datastore.edm;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.dataloom.datastore.BootstrapDatastoreWithCassandra;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.datastore.authentication.AuthenticatedRestCallsTest;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.EntityDataModel;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

public class EdmControllerTests extends BootstrapDatastoreWithCassandra {
    private final static Logger logger = LoggerFactory.getLogger( AuthenticatedRestCallsTest.class );
    private final EdmApi        edm    = getApiAdmin( EdmApi.class );

    public PropertyType createPropertyType() {
        PropertyType expected = TestDataFactory.propertyType();
        UUID propertyTypeId = edm.createPropertyType( expected );

        Assert.assertNotNull( "Property type creation returned null value.", propertyTypeId );

        PropertyType actual = edm.getPropertyType( propertyTypeId );

        Assert.assertNotNull( "Property type retrieval returned null value.", actual );
        Assert.assertEquals( "Created and retrieved property type don't match", expected, actual );

        return actual;
    }

    public EntityType createEntityType() {
        PropertyType p1 = createPropertyType();
        PropertyType k = createPropertyType();
        PropertyType p2 = createPropertyType();

        EntityType expected = TestDataFactory.entityType( k );
        expected.removePropertyTypes( expected.getProperties() );
        expected.addPropertyTypes( ImmutableSet.of( k.getId(), p1.getId(), p2.getId() ) );
        UUID entityTypeId = edm.createEntityType( expected );
        Assert.assertNotNull( "Entity type creation shouldn't return null UUID.", entityTypeId );

        return expected;
    }

    public EntitySet createEntitySet() {
        EntityType entityType = createEntityType();

        EntitySet es = new EntitySet(
                UUID.randomUUID(),
                entityType.getId(),
                TestDataFactory.name(),
                "foobar",
                Optional.<String> of( "barred" ),
                Optional.of( ImmutableSet.of( "foo@bar.com", "foobar@foo.net" ) ) );

        Set<EntitySet> ees = ImmutableSet.copyOf( edm.getEntitySets() );

        Assert.assertFalse( ees.contains( es ) );

        Map<String, UUID> entitySetIds = edm.createEntitySets( ImmutableSet.of( es ) );
        entitySetIds.values().contains( es.getId() );

        Set<EntitySet> aes = ImmutableSet.copyOf( edm.getEntitySets() );

        Assert.assertTrue( aes.contains( es ) );
        
        return es;
    }

    @Test
    public void testCreatePropertyType() {
        createPropertyType();
    }

    @Test
    public void testLookupPropertyTypeByFqn() {
        PropertyType propertyType = createPropertyType();
        UUID maybePropertyTypeId = edm.getPropertyTypeId(
                propertyType.getType().getNamespace(),
                propertyType.getType().getName() );
        Assert.assertNotNull( maybePropertyTypeId );
        Assert.assertEquals( propertyType.getId(), maybePropertyTypeId );
    }

    @Test
    public void testCreateEntityType() {
        createEntityType();
    }

    @Test
    public void testLookupEntityTypeByFqn() {
        EntityType entityType = createEntityType();
        UUID maybeEntityTypeId = edm.getEntityTypeId(
                entityType.getType().getNamespace(),
                entityType.getType().getName() );
        Assert.assertNotNull( maybeEntityTypeId );
        Assert.assertEquals( entityType.getId(), maybeEntityTypeId );
    }

    @Test
    public void testEntityDataModel() {
        EntityDataModel dm = edm.getEntityDataModel();
        Assert.assertNotNull( dm );
    }

    @Test
    public void testCreateEntitySet() {
        createEntitySet();
    }

    @Test
    public void testGetPropertyTypes() {
        Set<PropertyType> epts = ImmutableSet.copyOf( edm.getPropertyTypes() );
        PropertyType propertyType = createPropertyType();
        Assert.assertFalse( epts.contains( propertyType ) );
        Set<PropertyType> apts = ImmutableSet.copyOf( edm.getPropertyTypes() );
        Assert.assertTrue( apts.contains( propertyType ) );
    }

    @Test
    public void testRenameTypes() {
        PropertyType pt = createPropertyType();
        EntityType et = createEntityType();
        EntitySet es = createEntitySet();

        FullQualifiedName newPtFqn = TestDataFactory.fqn();
        FullQualifiedName newEtFqn = TestDataFactory.fqn();
        String newEsName = TestDataFactory.name();

        edm.renamePropertyType( pt.getId(), newPtFqn );
        edm.renameEntityType( et.getId(), newEtFqn );
        edm.renameEntitySet( es.getId(), newEsName );

        Assert.assertEquals( newPtFqn, edm.getPropertyType( pt.getId() ).getType() );
        Assert.assertEquals( newEtFqn, edm.getEntityType( et.getId() ).getType() );
        Assert.assertEquals( newEsName, edm.getEntitySet( es.getId() ).getName() );
    }

    @AfterClass
    public static void testsComplete() {
        logger.info( "This is for setting breakpoints." );
    }
}
