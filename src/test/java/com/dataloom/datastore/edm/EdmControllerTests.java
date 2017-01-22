package com.dataloom.datastore.edm;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.datastore.authentication.AuthenticatedRestCallsTest;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.EntityDataModel;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.mapstores.TestDataFactory;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

public class EdmControllerTests extends AuthenticatedRestCallsTest {
    private final static Logger logger = LoggerFactory.getLogger( AuthenticatedRestCallsTest.class );
    private final EdmApi        edm    = getApi( EdmApi.class );

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
    public void createEntitySet() {
        EntityType entityType = createEntityType();

        EntitySet es = new EntitySet(
                UUID.randomUUID(),
                entityType.getId(),
                "foo",
                "foobar",
                Optional.<String> of( "barred" ) );

        Set<EntitySet> ees = ImmutableSet.copyOf( edm.getEntitySets() );

        Assert.assertFalse( ees.contains( es ) );

        Map<String, UUID> entitySetIds = edm.createEntitySets( ImmutableSet.of( es ) );
        entitySetIds.values().contains( es.getId() );

        Set<EntitySet> aes = ImmutableSet.copyOf( edm.getEntitySets() );

        Assert.assertTrue( aes.contains( es ) );
    }

    @Test
    public void testGetPropertyTypes() {
        Set<PropertyType> epts = ImmutableSet.copyOf( edm.getPropertyTypes() );
        PropertyType propertyType = createPropertyType();
        Assert.assertFalse( epts.contains( propertyType ) );
        Set<PropertyType> apts = ImmutableSet.copyOf( edm.getPropertyTypes() );
        Assert.assertTrue( apts.contains( propertyType ) );
    }

    @AfterClass
    public static void testsComplete() {
        logger.info( "This is for setting breakpoints." );
    }
}
