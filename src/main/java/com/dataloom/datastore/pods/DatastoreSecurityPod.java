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

import com.dataloom.authentication.LoomAuth0AuthenticationProvider;
import com.dataloom.authorization.SystemRole;
import com.dataloom.organizations.roles.SecurePrincipalsManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.auth0.Auth0SecurityPod;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.openlattice.authentication.Auth0Configuration;
import javax.inject.Inject;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

@Configuration
@EnableGlobalMethodSecurity(
        prePostEnabled = true )
@EnableWebSecurity(
        debug = false )
@EnableMetrics
public class DatastoreSecurityPod extends Auth0SecurityPod {

    @Inject private
    ObjectMapper defaultObjectMapper;

    @Inject private SecurePrincipalsManager spm;

    @Inject private Auth0Configuration auth0Configuration;

    protected AuthenticationProvider getAuthenticationProvider() {
        return new LoomAuth0AuthenticationProvider(
                auth0Configuration.getClientSecret().getBytes(),
                auth0Configuration.getIssuer(),
                auth0Configuration.getAudience(),
                spm );
    }

    @Override protected void configure( HttpSecurity http ) throws Exception {
        super.configure( http );
        http.authenticationProvider( getAuthenticationProvider() );
    }

    @Override
    protected void authorizeRequests( HttpSecurity http ) throws Exception {
        //TODO: Lock these down
        http.authorizeRequests()
                .antMatchers( HttpMethod.OPTIONS ).permitAll()
                .antMatchers( HttpMethod.PUT, "/datastore/principals/users/*" ).permitAll()
                .antMatchers( HttpMethod.GET, "/datastore/edm/**" ).permitAll()
                .antMatchers( "/datastore/data/entitydata/*" ).permitAll()
                .antMatchers( "/datastore/**" ).hasAnyAuthority( SystemRole.valuesAsArray() );
    }
}
