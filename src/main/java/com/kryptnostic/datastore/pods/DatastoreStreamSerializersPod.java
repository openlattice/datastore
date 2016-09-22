package com.kryptnostic.datastore.pods;

import javax.inject.Inject;

import com.datastax.driver.core.Session;
import com.kryptnostic.conductor.rpc.serializers.QueryResultStreamSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kryptnostic.conductor.rpc.ConductorSparkApi;
import com.kryptnostic.conductor.rpc.LambdaStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.ConductorCallStreamSerializer;
import com.kryptnostic.conductor.rpc.serializers.EmployeeStreamSerializer;

@Configuration
public class DatastoreStreamSerializersPod {

    @Inject
    private Session session;

    private ConductorSparkApi api = null;

    @Bean
    public ConductorCallStreamSerializer ccss() {
        return new ConductorCallStreamSerializer( api );
    }


    @Bean
    public LambdaStreamSerializer lss() {
        return new LambdaStreamSerializer();
    }

    @Bean
    public QueryResultStreamSerializer qrss() {
        return new QueryResultStreamSerializer( session );
    }
}
