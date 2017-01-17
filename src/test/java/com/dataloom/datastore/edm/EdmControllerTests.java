package com.dataloom.datastore.edm;

import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.datastore.authentication.AuthenticedRestCallsTest;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.EntityDataModel;
import com.dataloom.edm.internal.AbstractSchemaAssociatedSecurableType;
import com.dataloom.edm.internal.AbstractSecurableType;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.internal.TestDataFactory;
import com.google.common.collect.ImmutableSet;

public class EdmControllerTests extends AuthenticedRestCallsTest {
    private final static Logger logger = LoggerFactory.getLogger( AuthenticedRestCallsTest.class );
    private final EdmApi        edm    = getApi( EdmApi.class );

    public Pair<UUID, PropertyType> createPropertyType() {
        PropertyType p = TestDataFactory.propertyType();
        UUID propertyTypeId = edm.createPropertyType( p );
        Assert.assertNotNull( "Property type creation shouldn't return null UUID.", propertyTypeId );
        return Pair.of( propertyTypeId, p );
    }

    public Pair<UUID, EntityType> createEntityType() {
        Pair<UUID, PropertyType> k = createPropertyType();
        Pair<UUID, PropertyType> p1 = createPropertyType();
        Pair<UUID, PropertyType> p2 = createPropertyType();

        EntityType e = TestDataFactory.entityType( k.getRight() );
        e.removePropertyTypes( e.getProperties() );
        e.addPropertyTypes( ImmutableSet.of( k.getLeft(), p1.getLeft(), p2.getLeft() ) );
        UUID entityTypeId = edm.createEntityType( e );
        Assert.assertNotNull( "Entity type creation shouldn't return null UUID.", entityTypeId );
        return Pair.of( entityTypeId, e );
    }

    public void compareAST( AbstractSecurableType expected, AbstractSecurableType actual ) {
        Assert.assertEquals( expected.getTitle(), actual.getTitle() );
        Assert.assertEquals( expected.getDescription(), actual.getDescription() );
        Assert.assertEquals( expected.getCategory(), actual.getCategory() );
        Assert.assertEquals( expected.getType(), actual.getType() );
    }

    public void compareSAST(
            AbstractSchemaAssociatedSecurableType expected,
            AbstractSchemaAssociatedSecurableType actual ) {
        compareAST( expected, actual );
        Assert.assertEquals( expected.getSchemas(), actual.getSchemas() );
    }

    public void compare( PropertyType expected, PropertyType actual ) {
        compareSAST( expected, actual );
        Assert.assertEquals( expected.getDatatype(), actual.getDatatype() );
    }

    public void compare( EntityType expected, EntityType actual ) {
        compareSAST( expected, actual );
        Assert.assertEquals( expected.getProperties(), actual.getProperties() );
        Assert.assertEquals( expected.getKey(), actual.getKey() );
    }

    @Test
    public void testCreatePropertyType() {
        Pair<UUID, PropertyType> propertyTypePair = createPropertyType();
        PropertyType p = edm.getPropertyType( propertyTypePair.getLeft() );
        Assert.assertNotNull( p );
        compare( propertyTypePair.getRight(), p );
    }

    @Test
    public void testLookupPropertyTypeByFqn() {
        Pair<UUID, PropertyType> propertyTypePair = createPropertyType();
        PropertyType propertyType = edm.getPropertyType( propertyTypePair.getLeft() );
        Assert.assertNotNull( propertyType );
        UUID maybePropertyTypeId = edm.getPropertyTypeId(
                propertyType.getType().getNamespace(),
                propertyType.getType().getName() );
        Assert.assertNotNull( maybePropertyTypeId );
        Assert.assertEquals( propertyTypePair.getLeft(), maybePropertyTypeId );
    }

    @Test
    public void testCreateEntityType() {
        Pair<UUID, EntityType> entityTypePair = createEntityType();
        EntityType entityType = edm.getEntityType( entityTypePair.getLeft() );
        UUID maybeEntityTypeId = edm.getEntityTypeId(
                entityType.getType().getNamespace(),
                entityType.getType().getName() );
        Assert.assertNotNull( maybeEntityTypeId );
        Assert.assertEquals( entityTypePair.getLeft(), maybeEntityTypeId );
    }

    @Test
    public void testLookupEntityTypeByFqn() {
        Pair<UUID, EntityType> entityTypePair = createEntityType();
        EntityType entityType = edm.getEntityType( entityTypePair.getLeft() );
        UUID maybeEntityTypeId = edm.getEntityTypeId(
                entityType.getType().getNamespace(),
                entityType.getType().getName() );
        Assert.assertEquals( entityTypePair.getLeft(), maybeEntityTypeId );
    }
    
    @Test
    public void testEntityDataModel() {
        EntityDataModel dm = edm.getEntityDataModel();
        Assert.assertNotNull( dm ); 
    }

    @AfterClass
    public static void testsComplete() {
        logger.info( "This is for setting breakpoints." );
    }
}
