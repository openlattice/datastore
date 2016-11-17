package com.kryptnostic.datastore.serialization;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.data.serializers.FullQualifedNameJacksonSerializer;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
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
        return new EntityType().setNamespace( "test" ).setName( "andino" )
                .setProperties( ImmutableSet.of( new FullQualifiedName( "test", "pan flute" ) ) )
                .setKey( ImmutableSet.of( new FullQualifiedName( "test", "id" ) ) );
    }

    @Override
    protected Class<EntityType> getClazz() {
        return EntityType.class;
    }

    @Test
    public void testSettingsAreIgnored() throws Exception {
        EntityType data = getSampleData();
        SerializationResult result = serialize( data.setTypename( "blah" ) );
        EntityType deserializedData = deserializeJsonBytes( result );
        Assert.assertNotNull( data.getTypename() );
        Assert.assertNull( deserializedData.getTypename() );
    }
}
