package com.dataloom.datastore.edm;

import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import com.dataloom.datastore.authentication.AuthenticedRestCallsTest;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.internal.AbstractSchemaAssociatedSecurableType;
import com.dataloom.edm.internal.AbstractSecurableType;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.internal.TestDataFactory;

public class EdmControllerTests extends AuthenticedRestCallsTest {
    private final EdmApi edm = getApi( EdmApi.class );

    public Pair<UUID, PropertyType> createPropertyType() {
        PropertyType p = TestDataFactory.propertyType();
        UUID propertyTypeId = edm.createPropertyType( p );
        Assert.assertNotNull( "Property type creation shouldn't return null UUID.", p );
        return Pair.of( propertyTypeId, p );
    }

    public Pair<UUID, EntityType> createEntityType() {
        EntityType e = TestDataFactory.entityType();
        UUID entityTypeId = edm.createEntityType( e );
        Assert.assertNotNull( "Entity type creation shouldn't return null UUID.", e );
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
        compare( propertyTypePair.getRight(), p );
    }

    @Test
    public void testLookupPropertyTypeByFqn() {
        Pair<UUID, PropertyType> propertyTypePair = createPropertyType();
        PropertyType propertyType = edm.getPropertyType( propertyTypePair.getLeft() );
        UUID maybePropertyTypeId = edm.getPropertyTypeId(
                propertyType.getType().getNamespace(),
                propertyType.getType().getName() );
        Assert.assertEquals( propertyTypePair.getLeft(), maybePropertyTypeId );
    }

    @Test
    public void testCreateEntityType() {
        Pair<UUID, EntityType> entityTypePair = createEntityType();
        EntityType entityType = edm.getEntityType( entityTypePair.getLeft() );
        UUID maybeEntityTypeId = edm.getEntityTypeId(
                entityType.getType().getNamespace(),
                entityType.getType().getName() );
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

}
