package com.kryptnostic.datastore.serialization;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.BeforeClass;

import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.data.serializers.FullQualifedNameJacksonSerializer;
import com.dataloom.edm.internal.PropertyType;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.hazelcast.serializers.BaseJacksonSerializationTest;
import com.kryptnostic.rhizome.registries.ObjectMapperRegistry;

public class PropertyTypeSerializerTest extends BaseJacksonSerializationTest<PropertyType> {

    @BeforeClass
    public static void configureSerializer() {
        ObjectMapperRegistry.foreach( FullQualifedNameJacksonSerializer::registerWithMapper );
        ObjectMapperRegistry.foreach( FullQualifedNameJacksonDeserializer::registerWithMapper );
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
