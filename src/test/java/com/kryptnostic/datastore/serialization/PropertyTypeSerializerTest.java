package com.kryptnostic.datastore.serialization;

import java.io.IOException;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.data.serializers.FullQualifedNameJacksonSerializer;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
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
        return new PropertyType().setNamespace( "test" ).setName( "andino" )
                .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 );
    }

    @Override
    protected Class<PropertyType> getClazz() {
        return PropertyType.class;
    }

    @Test
    public void testSettingsAreIgnored() throws Exception {
        PropertyType data = getSampleData();
        SerializationResult result = serialize( data.setTypename( "blah" ) );
        PropertyType deserializedData = deserializeJsonBytes( result );
        Assert.assertNotNull( data.getTypename() );
        Assert.assertNull( deserializedData.getTypename() );
    }

    @Test
    public void deserializationWithTypename() throws IOException {
        final String json = "{ \n"
                + " \"namespace\" : \"test \", \n"
                + " \"name\" : \"andino \", \n"
                + " \"datatype\" : \"" + EdmPrimitiveTypeKind.String.toString() + "\", \n"
                + " \"multiplicity\" : 0  \n}";
        PropertyType propertyType = deserializeJsonString( new SerializationResult().setJsonString( json ) );
        Assert.assertNull( propertyType.getTypename() );
    }
}
