package com.kryptnostic.types.pods;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.Lists;
import com.kryptnostic.rhizome.configuration.servlets.DispatcherServletConfiguration;

@Configuration
public class TypesServletsPod {
    @Bean
    public DispatcherServletConfiguration typesServlet() {
        return new DispatcherServletConfiguration(
                "types",
                new String[] { "/types/*" },
                1,
                Lists.<Class<?>> newArrayList( TypesMvcPod.class ) );
    }
    
    @Bean
    public DispatcherServletConfiguration odataServlet() {
        return new DispatcherServletConfiguration(
                "odata",
                new String[] { "/odata/*" },
                1,
                Lists.<Class<?>> newArrayList( ODataMvcPod.class ) );
    }

}
