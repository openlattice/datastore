package com.dataloom.datastore.pods;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.dataloom.datastore.services.DatastoreConductorSparkApi;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;

@Configuration
public class SparkDependencyPod {
    @Bean
    public ConductorSparkApi api() {
        return new DatastoreConductorSparkApi();
    }
}
