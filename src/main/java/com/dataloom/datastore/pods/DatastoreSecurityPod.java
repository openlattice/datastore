/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.datastore.pods;

import javax.inject.Inject;

import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

import com.dataloom.authentication.LoomAuth0AuthenticationProvider;
import com.dataloom.organizations.roles.TokenExpirationTracker;

import digital.loom.rhizome.authentication.Auth0SecurityPod;
import digital.loom.rhizome.authentication.ConfigurableAuth0AuthenticationProvider;

@Configuration
@EnableGlobalMethodSecurity(
    prePostEnabled = true )
@EnableWebSecurity(
    debug = false )
public class DatastoreSecurityPod extends Auth0SecurityPod {

    @Inject
    TokenExpirationTracker tokenTracker;
    
    @Override
    protected ConfigurableAuth0AuthenticationProvider getAuthenticationProvider() {
        return new LoomAuth0AuthenticationProvider( getAuthenticationApiClient(), tokenTracker );
    }

    @Override
    protected void authorizeRequests( HttpSecurity http ) throws Exception {
        http.authorizeRequests()
                .antMatchers( HttpMethod.OPTIONS ).permitAll()
                .antMatchers( "/datastore/data/entitydata/*" ).permitAll()
                .antMatchers( "/datastore/**" ).hasAnyAuthority( "admin", "ADMIN", "AuthenticatedUser" );
//                .antMatchers( "/datastore/**" ).hasAnyAuthority( "AuthenticatedUser", "AuthenticatedUser" );
    }
}
