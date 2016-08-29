package com.kryptnostic.types.pods;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kryptnostic.conductor.rpc.LambdaStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.ConductorCallStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.EmployeeStreamSerializer;

@Configuration
public class DatastoreStreamSerializersPod {

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
