package com.kryptnostic.datastore.pods;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.Lists;
import com.kryptnostic.rhizome.configuration.servlets.DispatcherServletConfiguration;

@Configuration
public class DatastoreServletsPod {
    @Bean
    public DispatcherServletConfiguration odataServlet() {
        return new DispatcherServletConfiguration(
                "odata",
                new String[] { "/odata/*" },
                1,
                Lists.<Class<?>> newArrayList( ODataMvcPod.class ) );
    }

    @Bean
    public DispatcherServletConfiguration ontologyServlet() {
        return new DispatcherServletConfiguration(
                "ontology",
                new String[] { "/ontology/*" },
                1,
                Lists.<Class<?>> newArrayList( EdmMvcPod.class ) );
    }
}
