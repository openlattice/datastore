package com.dataloom.datastore.pods;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.Lists;
import com.kryptnostic.rhizome.configuration.servlets.DispatcherServletConfiguration;

@Configuration
public class DatastoreServletsPod {

    @Bean
    public DispatcherServletConfiguration directoryServlet() {
        return new DispatcherServletConfiguration(
                "directory",
                new String[] { "/directory/*" },
                1,
                Lists.<Class<?>> newArrayList( DirectoryMvcPod.class ) );
    }

    @Bean
    public DispatcherServletConfiguration odataServlet() {
        return new DispatcherServletConfiguration(
                "odata",
                new String[] { "/odata/*" },
                1,
                Lists.<Class<?>> newArrayList( ODataMvcPod.class ) );
    }

    @Bean
    public DispatcherServletConfiguration permissionsServlet() {
        return new DispatcherServletConfiguration(
                "permissions",
                new String[] { "/permissions/*" },
                1,
                Lists.<Class<?>> newArrayList( PermissionMvcPod.class ) );
    }

    @Bean
    public DispatcherServletConfiguration searchServlet() {
        return new DispatcherServletConfiguration(
                "search",
                new String[] { "/search/*" },
                1,
                Lists.<Class<?>> newArrayList( SearchMvcPod.class ) );
    }

    @Bean
    public DispatcherServletConfiguration dataServlet() {
        return new DispatcherServletConfiguration(
                "data",
                new String[] { "/data/*" },
                1,
                Lists.<Class<?>> newArrayList( DataMvcPod.class ) );
    }

    @Bean
    public DispatcherServletConfiguration edmServlet() {
        return new DispatcherServletConfiguration(
                "ontology",
                new String[] { "/ontology/*" },
                1,
                Lists.<Class<?>> newArrayList( EdmMvcPod.class ) );
    }
    
    @Bean
    public DispatcherServletConfiguration authorizationsServlet() {
        return new DispatcherServletConfiguration(
                "authorizations",
                new String[] { "/authorizations/*" },
                1,
                Lists.<Class<?>> newArrayList( AuthorizationsMvcPod.class ) );
    }
}
