package com.dataloom.datastore.pods;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

import com.auth0.spring.security.api.Auth0AuthenticationProvider;
import com.dataloom.authentication.LoomAuth0AuthenticationProvider;

import digital.loom.rhizome.authentication.Auth0SecurityPod;
import digital.loom.rhizome.authentication.ConfigurableAuth0AuthenticationProvider;

@Configuration
@EnableGlobalMethodSecurity(
    prePostEnabled = true )
@EnableWebSecurity(
    debug = false )
public class DatastoreSecurityPod extends Auth0SecurityPod {
    
    @Override
    protected ConfigurableAuth0AuthenticationProvider getAuthenticationProvider() {
        return new LoomAuth0AuthenticationProvider( getAuthenticationApiClient() );
    }

    @Override
    protected void authorizeRequests( HttpSecurity http ) throws Exception {
        http.authorizeRequests()
                .antMatchers( HttpMethod.OPTIONS ).permitAll()
                .antMatchers( "/datastore/**" ).hasAnyAuthority( "user", "USER" )
                .antMatchers( "/datastore/**" ).hasAnyAuthority( "admin", "ADMIN" );
    }
}
