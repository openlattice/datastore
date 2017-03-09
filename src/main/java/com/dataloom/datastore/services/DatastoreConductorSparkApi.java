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

package com.dataloom.datastore.services;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.data.requests.LookupEntitiesRequest;
import com.dataloom.edm.type.PropertyType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.conductor.rpc.ConductorCall;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;
import com.kryptnostic.conductor.rpc.Lambdas;
import com.kryptnostic.conductor.rpc.QueryResult;

public class DatastoreConductorSparkApi implements ConductorSparkApi {

    private static final Logger          logger = LoggerFactory.getLogger( DatastoreConductorSparkApi.class );

    private final DurableExecutorService executor;

    public DatastoreConductorSparkApi( HazelcastInstance hazelcast ) {
        this.executor = hazelcast.getDurableExecutorService( "default" );
    }

    @Override
    public QueryResult getAllEntitiesOfType( FullQualifiedName entityTypeFqn ) {
        return getAllEntitiesOfType( entityTypeFqn, null );
    }

    @Override
    public QueryResult getAllEntitiesOfType(
            FullQualifiedName entityTypeFqn,
            List<PropertyType> authorizedProperties ) {
        try {
            return executor.submit( ConductorCall.wrap( Lambdas.getAllEntitiesOfType( entityTypeFqn, authorizedProperties ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute get all entities of type" );
            return new QueryResult( null, null, null, null );
        }
    }

    @Override
    public QueryResult getAllEntitiesOfEntitySet( FullQualifiedName entityFqn, String entitySetName ) {

        return null;
    }

    @Override
    public QueryResult getAllEntitiesOfEntitySet(
            FullQualifiedName entityFqn,
            String entitySetName,
            List<PropertyType> authorizedProperties ) {
        try {
            return executor.submit( ConductorCall.wrap( Lambdas.getAllEntitiesOfEntitySet( entityFqn, entitySetName, authorizedProperties ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute get all entities of entity set" );
            return new QueryResult( null, null, null, null );
        }
    }

    @Override
    public QueryResult getFilterEntities( LookupEntitiesRequest request ) {
        try {
            return executor.submit( ConductorCall.wrap( Lambdas.getFilteredEntities( request ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute get all entities of entity set" );
            return new QueryResult( null, null, null, null );
        }
    }

    @Override
    public Void clustering( UUID linkedEntitySetId ) {
        try {
            return executor.submit( ConductorCall.wrap( Lambdas.clustering( linkedEntitySetId ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute clustering" );
            return null;
        }
    }

    @Override
    public UUID getTopUtilizers( UUID entitySetId, UUID propertyTypeId, Map<UUID, PropertyType> propertyTypes ) {
        try {
            return executor.submit( ConductorCall.wrap( Lambdas.getTopUtilizers( entitySetId, propertyTypeId, propertyTypes ) ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "unable to execute get top utilizers" );
            return null;
        }
    }

}