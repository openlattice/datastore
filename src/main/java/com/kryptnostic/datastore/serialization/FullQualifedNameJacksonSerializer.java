package com.kryptnostic.datastore.serialization;

import java.io.IOException;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.kryptnostic.conductor.rpc.odata.SerializationConstants;

public class FullQualifedNameJacksonSerializer extends StdSerializer<FullQualifiedName>  {

    protected FullQualifedNameJacksonSerializer() {
        this( null );
    }

    public FullQualifedNameJacksonSerializer( Class<FullQualifiedName> clazz ) {
        super( clazz );
    }

    @Override
    public void serialize( FullQualifiedName value, JsonGenerator jgen, SerializerProvider provider )
            throws IOException, JsonGenerationException {
        jgen.writeStartObject();
        jgen.writeStringField( SerializationConstants.NAMESPACE_FIELD, value.getNamespace() );
        jgen.writeStringField( SerializationConstants.NAME_FIELD, value.getName() );
        jgen.writeEndObject();
    }

    
    public static void registerWithMapper( ObjectMapper mapper ) {
        SimpleModule module = new SimpleModule();
        module.addSerializer( FullQualifiedName.class, new FullQualifedNameJacksonSerializer() );
        mapper.registerModule( module );
    }
}
