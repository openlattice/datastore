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

package com.dataloom.datastore;

import java.util.Set;

import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.auth0.spring.security.api.Auth0JWTToken;
import com.dataloom.authentication.LoomAuth0AuthenticationProvider;
import com.dataloom.authentication.LoomAuthentication;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.Principal;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.edm.schemas.manager.HazelcastSchemaManager;
import com.geekbeast.rhizome.tests.bootstrap.CassandraBootstrap;
import com.google.common.collect.Sets;
import com.dataloom.data.storage.CassandraEntityDatastore;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.rhizome.pods.SparkPod;

import com.openlattice.authentication.AuthenticationTest;
import com.openlattice.authentication.AuthenticationTestRequestOptions;
import retrofit2.Retrofit;

public class BootstrapDatastoreWithCassandra extends CassandraBootstrap {
    protected static final Datastore                 ds       = new Datastore();
    protected static final Set<Class<?>>             PODS     = Sets.newHashSet( SparkPod.class );
    protected static final Set<String>               PROFILES = Sets.newHashSet( "local", "cassandra" );
    protected static final Principal                 admin;
    protected static final Principal                 user1;
    protected static final Principal                 user2;
    protected static final Principal                       user3;
    protected static final Retrofit                        retrofit;
    protected static final Retrofit                        retrofit1;
    protected static final Retrofit                        retrofit2;
    protected static final Retrofit                        retrofit3;
    protected static       LoomAuth0AuthenticationProvider loomAuthProvider;
    protected static       EdmManager                      dms;
    protected static       AuthorizationManager            am;
    protected static       CassandraEntityDatastore        dataService;
    protected static       HazelcastSchemaManager          schemaManager;

    protected static final AuthenticationTestRequestOptions authOptions1 = new AuthenticationTestRequestOptions()
            .setUsernameOrEmail( "tests1@kryptnostic.com" )
            .setPassword( "abracadabra" );
    protected static final AuthenticationTestRequestOptions authOptions2 = new AuthenticationTestRequestOptions()
            .setUsernameOrEmail( "tests2@kryptnostic.com" )
            .setPassword( "abracadabra" );
    protected static final AuthenticationTestRequestOptions authOptions3 = new AuthenticationTestRequestOptions()
            .setUsernameOrEmail( "tests3@kryptnostic.com" )
            .setPassword( "abracadabra" );
    
    static {
        String jwtAdmin = AuthenticationTest.authenticate().getCredentials().getIdToken();
        String jwtUser1 = AuthenticationTest.getAuthentication( authOptions1 ).getCredentials().getIdToken();
        String jwtUser2 = AuthenticationTest.getAuthentication( authOptions2 ).getCredentials().getIdToken();
        String jwtUser3 = AuthenticationTest.getAuthentication( authOptions3 ).getCredentials().getIdToken();

        retrofit = RetrofitFactory.newClient( RetrofitFactory.Environment.TESTING, () -> jwtAdmin );
        retrofit1 = RetrofitFactory.newClient( RetrofitFactory.Environment.TESTING, () -> jwtUser1 );
        retrofit2 = RetrofitFactory.newClient( RetrofitFactory.Environment.TESTING, () -> jwtUser2 );
        retrofit3 = RetrofitFactory.newClient( RetrofitFactory.Environment.TESTING, () -> jwtUser3 );

        try {
            ds.intercrop( PODS.toArray( new Class<?>[ 0 ] ) );
            ds.start( PROFILES.toArray( new String[ 0 ] ) );
        } catch ( Exception e ) {
            throw new IllegalStateException( "Unable to start datastore", e );
        }

        loomAuthProvider = getBean( LoomAuth0AuthenticationProvider.class );
        dms = getBean( EdmManager.class );
        am = getBean( AuthorizationManager.class );
        dataService = getBean( CassandraEntityDatastore.class );
        schemaManager = getBean( HazelcastSchemaManager.class );

        admin = toPrincipal( jwtAdmin );
        user1 = toPrincipal( jwtUser1 );
        user2 = toPrincipal( jwtUser2 );
        user3 = toPrincipal( jwtUser3 );

        TestEdmConfigurer.setupDatamodel( admin, dms, schemaManager );
    }

    protected final Logger logger = LoggerFactory.getLogger( getClass() );

    private static Principal toPrincipal( String jwtToken ) {
        return ( (LoomAuthentication) loomAuthProvider
                .authenticate( new Auth0JWTToken( jwtToken ) ) )
                        .getLoomPrincipal();
    }

    protected static <T> T getBean( Class<T> clazz ) {
        return ds.getContext().getBean( clazz );
    }

    protected static <T> T getApiAdmin( Class<T> clazz ) {
        return retrofit.create( clazz );
    }

    protected static <T> T getApiUser1( Class<T> clazz ) {
        return retrofit1.create( clazz );
    }

    protected static <T> T getApiUser2( Class<T> clazz ) {
        return retrofit2.create( clazz );
    }

    protected static <T> T getApiUser3( Class<T> clazz ) {
        return retrofit3.create( clazz );
    }

    protected static Principal getUser1() {
        return user1;
    }

    protected static Principal getUser2() {
        return user2;
    }

    protected static Principal getUser3() {
        return user3;
    }

    @AfterClass
    public static void shutdown() {
        LoggerFactory.getLogger( BootstrapDatastoreWithCassandra.class ).info( "BREAKPOINT" );
    }
}
