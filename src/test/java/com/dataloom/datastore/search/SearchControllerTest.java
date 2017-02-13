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

package com.dataloom.datastore.search;

import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dataloom.datastore.authentication.MultipleAuthenticatedUsersBase;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.EntityType;
import com.dataloom.mappers.ObjectMappers;
import com.dataloom.search.requests.SearchRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;

@Ignore
public class SearchControllerTest extends MultipleAuthenticatedUsersBase {

    // All random entity sets generated has title foobar and description barred
    private static final String title       = "foobar";
    private static final String description = "barred";

    private static EntityType   et1;
    private static EntityType   et2;
    private static EntitySet    es1;
    private static EntitySet    es2;

    @BeforeClass
    public static void init() {
        loginAs( "admin" );
        et1 = createEntityType();
        et2 = createEntityType();
        es1 = createEntitySet( et1 );
        es2 = createEntitySet( et2 );

        System.out.println( "Entity Types created: " );
        System.out.println( et1 );
        System.out.println( et2 );

        System.out.println( "Entity Sets created: " );
        System.out.println( es1 );
        System.out.println( es2 );
        try {
            Thread.sleep( 5000 );
        } catch ( InterruptedException e ) {
            System.err.println( "The wait for elastic search indexing is disrupted." );
        }

    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void testSearchByEntityType() {
        SearchRequest req = new SearchRequest( Optional.absent(), Optional.of( et1.getId() ), Optional.absent() );
        Iterable<Map<String, Object>> results = searchApi.executeQuery( req );

        Assert.assertEquals( 1, Iterables.size( results ) );
        Map<String, Object> result = (Map) results.iterator().next().get( ConductorElasticsearchApi.ENTITY_SET );
        Assert.assertEquals( es1.getName(), (String) result.get( "name" ) );
    }

    @Test
    public void testSearchByPropertyType() {
        SearchRequest req = new SearchRequest(
                Optional.absent(),
                Optional.absent(),
                Optional.of( et1.getProperties() ) );
        Iterable<Map<String, Object>> results = searchApi.executeQuery( req );

        Assert.assertEquals( 1, Iterables.size( results ) );
        Map<String, Object> result = (Map) results.iterator().next().get( ConductorElasticsearchApi.ENTITY_SET );
        Assert.assertEquals( es1.getName(), (String) result.get( ConductorElasticsearchApi.NAME ) );
    }

    @Test
    public void testSearchByTitleKeyword() {
        SearchRequest req = new SearchRequest( Optional.of( title ), Optional.absent(), Optional.absent() );
        Iterable<Map<String, Object>> results = searchApi.executeQuery( req );

        // Filter search results to those that matches names of the two entity sets we created
        Set<String> names = ImmutableSet.of( es1.getName(), es2.getName() );
        Iterable<Map<String, Object>> filtered = Iterables.filter( results, result -> matchName( result, names ) );

        Assert.assertEquals( 2, Iterables.size( filtered ) );
    }

    @Test
    public void testSearchByDescriptionKeyword() {
        SearchRequest req = new SearchRequest( Optional.of( description ), Optional.absent(), Optional.absent() );
        Iterable<Map<String, Object>> results = searchApi.executeQuery( req );

        // Filter search results to those that matches names of the two entity sets we created
        Set<String> names = ImmutableSet.of( es1.getName(), es2.getName() );
        Iterable<Map<String, Object>> filtered = Iterables.filter( results, result -> matchName( result, names ) );

        Assert.assertEquals( 2, Iterables.size( filtered ) );
    }

    // @Test
    public void testEndpointsConsistency() throws JsonProcessingException {
        SearchRequest req = new SearchRequest(
                Optional.absent(),
                Optional.absent(),
                Optional.of( et1.getProperties() ) );
        Iterable<Map<String, Object>> expected = searchApi.executeQuery( req );

        String expectedJson = ObjectMappers.getJsonMapper().writeValueAsString( expected );
        Assert.assertEquals( searchApi.executeQueryJson( req ), expectedJson );

    }

    @SuppressWarnings( "unchecked" )
    private boolean matchName( Map<String, Object> result, Set<String> names ) {
        Map<String, Object> entitySet = (Map<String, Object>) result.get( ConductorElasticsearchApi.ENTITY_SET );
        return names.contains( (String) entitySet.get( ConductorElasticsearchApi.NAME ) );
    }
}
