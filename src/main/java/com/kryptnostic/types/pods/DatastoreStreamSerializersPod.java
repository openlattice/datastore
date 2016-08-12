package com.kryptnostic.types.pods;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kryptnostic.datastore.odata.EntitySchemaStreamSerializer;
import com.kryptnostic.datastore.odata.Ontology.EntitySchema;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Configuration
public class DatastoreStreamSerializersPod {
    @Bean
    public SelfRegisteringStreamSerializer<EntitySchema> esss() {
        return new EntitySchemaStreamSerializer();
    }
}
