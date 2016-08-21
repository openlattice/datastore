package com.kryptnostic.types.pods;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kryptnostic.conductor.rpc.LambdaStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.ConductorCallStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.EmployeeStreamSerializer;
import com.kryptnostic.datastore.odata.EntitySchemaStreamSerializer;
import com.kryptnostic.datastore.odata.Ontology.EntitySchema;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

@Configuration
public class DatastoreStreamSerializersPod {
    @Bean
    public SelfRegisteringStreamSerializer<EntitySchema> esss() {
        return new EntitySchemaStreamSerializer();
    }

    @Bean
    public ConductorCallStreamSerializer ccss() {
        return new ConductorCallStreamSerializer( null );
    }

    @Bean
    public EmployeeStreamSerializer ess() {
        return new EmployeeStreamSerializer();
    }

    @Bean
    public LambdaStreamSerializer lss() {
        return new LambdaStreamSerializer();
    }
}
