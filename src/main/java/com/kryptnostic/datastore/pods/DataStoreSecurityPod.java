package com.kryptnostic.datastore.pods;

import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

import digital.loom.rhizome.authentication.Auth0SecurityPod;

@Configuration
@EnableGlobalMethodSecurity(
        prePostEnabled = true )
@EnableWebSecurity(
        debug = false )
public class DataStoreSecurityPod extends Auth0SecurityPod {

    @Override
    protected void authorizeRequests( HttpSecurity http ) throws Exception {
        http.authorizeRequests()
                .antMatchers( HttpMethod.OPTIONS ).permitAll()
                .antMatchers( "/ontology/data/**" ).hasAnyAuthority( "user", "USER" )
                .antMatchers( "/ontology/**" ).hasAnyAuthority( "admin", "ADMIN" )
                .antMatchers( "/odata/**" ).hasAnyAuthority( "user", "USER" );
    }
}
