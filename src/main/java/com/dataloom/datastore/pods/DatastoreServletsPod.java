package com.dataloom.datastore.pods;

import com.google.common.collect.Lists;
import com.kryptnostic.rhizome.configuration.servlets.DispatcherServletConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatastoreServletsPod {

    @Bean
    public DispatcherServletConfiguration edmServlet() {

        return new DispatcherServletConfiguration(
                "datastore",
                new String[] { "/datastore/*" },
                1,
                Lists.<Class<?>>newArrayList( DatastoreMvcPod.class ) );
    }
        return null;
}
}
