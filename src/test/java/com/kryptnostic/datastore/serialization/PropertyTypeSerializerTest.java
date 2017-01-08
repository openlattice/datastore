package com.kryptnostic.datastore.serialization;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.BeforeClass;

import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.data.serializers.FullQualifedNameJacksonSerializer;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.serializer.BaseJacksonSerializationTest;
import com.google.common.collect.ImmutableSet;

public class PropertyTypeSerializerTest extends BaseJacksonSerializationTest<PropertyType> {
    @BeforeClass
    public static void configureSerializer() {
        FullQualifedNameJacksonSerializer.registerWithMapper( mapper );
        FullQualifedNameJacksonDeserializer.registerWithMapper( mapper );
        FullQualifedNameJacksonSerializer.registerWithMapper( smile );
        FullQualifedNameJacksonDeserializer.registerWithMapper( smile );
    }

    @Override
    protected PropertyType getSampleData() {
        return new PropertyType(
                new FullQualifiedName( "test", "andino" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String );
    }

    @Override
    protected Class<PropertyType> getClazz() {
        return PropertyType.class;
    }
}
