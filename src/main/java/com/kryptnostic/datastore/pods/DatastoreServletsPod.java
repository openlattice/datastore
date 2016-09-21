package com.kryptnostic.datastore.pods;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.Lists;
import com.kryptnostic.rhizome.configuration.servlets.DispatcherServletConfiguration;

@Configuration
public class DatastoreServletsPod {
    @Bean
    public DispatcherServletConfiguration dataStoreServlet() {
        return new DispatcherServletConfiguration(
                "v1",
                new String[] { "/v1/*" },
                1,
                Lists.<Class<?>> newArrayList( DataStoreMvcPod.class ) );
    }

    @Bean
    public DispatcherServletConfiguration ontologyServlet() {
        return new DispatcherServletConfiguration(
                "ontology",
                new String[] { "/ontology/*" },
                1,
                Lists.<Class<?>> newArrayList( DataStoreMvcPod.class ) );
    }
}
