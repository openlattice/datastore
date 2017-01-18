package com.dataloom.datastore.pods;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.Lists;
import com.kryptnostic.rhizome.configuration.servlets.DispatcherServletConfiguration;

@Configuration
public class DatastoreServletsPod {

    @Bean
    public DispatcherServletConfiguration edmServlet() {
        return new DispatcherServletConfiguration(
                "edm",
                new String[] { "/datastore/*" },
                1,
                Lists.<Class<?>> newArrayList( DatastoreMvcPod.class ) );
    }

}
