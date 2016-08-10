package com.kryptnostic.types.pods;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;
import com.kryptnostic.types.odata.EntitySchemaStreamSerializer;
import com.kryptnostic.types.odata.Ontology.EntitySchema;

@Configuration
public class DatastoreStreamSerializersPod {
    @Bean
    public SelfRegisteringStreamSerializer<EntitySchema> esss() {
        return new EntitySchemaStreamSerializer();
    }
}
