package com.kryptnostic.datastore.serialization;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.BeforeClass;

import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.data.serializers.FullQualifedNameJacksonSerializer;
import com.dataloom.edm.internal.EntityType;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.rhizome.hazelcast.serializers.BaseJacksonSerializationTest;
import com.kryptnostic.rhizome.registries.ObjectMapperRegistry;

public class EntityTypeSerializerTest extends BaseJacksonSerializationTest<EntityType> {

    @BeforeClass
    public static void configureSerializer() {
        ObjectMapperRegistry.foreach( FullQualifedNameJacksonSerializer::registerWithMapper );
        ObjectMapperRegistry.foreach( FullQualifedNameJacksonDeserializer::registerWithMapper );
    }

    @Override
    protected EntityType getSampleData() {
        return new EntityType(
                new FullQualifiedName( "test", "andino" ),
                ImmutableSet.of( new FullQualifiedName( "test", "huepa" ) ),
                ImmutableSet.of( new FullQualifiedName( "test", "id" ) ),
                ImmutableSet.of( new FullQualifiedName( "test", "pan flute" ) ) );
    }

    @Override
    protected Class<EntityType> getClazz() {
        return EntityType.class;
    }
}