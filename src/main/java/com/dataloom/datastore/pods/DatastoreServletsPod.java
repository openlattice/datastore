package com.dataloom.datastore.pods;

import java.util.Set;

import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.kryptnostic.rhizome.configuration.ConfigurationConstants.Profiles;
import com.kryptnostic.rhizome.configuration.servlets.DispatcherServletConfiguration;

@Configuration
public class DatastoreServletsPod {

    @Inject
    private Environment environment;

    @Bean
    public DispatcherServletConfiguration edmServlet() {
        Set<String> profiles = ImmutableSet.copyOf( environment.getActiveProfiles() );

        if ( profiles.contains( Profiles.AWS_CONFIGURATION_PROFILE ) ) {
            return new DispatcherServletConfiguration(
                    "edm",
                    new String[] { "/*" },
                    1,
                    Lists.<Class<?>> newArrayList( DatastoreMvcPod.class ) );
        } else if ( profiles.contains( Profiles.LOCAL_CONFIGURATION_PROFILE ) ) {
            return new DispatcherServletConfiguration(
                    "edm",
                    new String[] { "/datastore/*" },
                    1,
                    Lists.<Class<?>> newArrayList( DatastoreMvcPod.class ) );
        }
        return null;
    }
}
