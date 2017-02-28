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
import java.util.UUID;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.dataloom.data.requests.LookupEntitiesRequest;
import com.dataloom.edm.type.PropertyType;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;
import com.kryptnostic.conductor.rpc.QueryResult;

public class DatastoreConductorSparkApi implements ConductorSparkApi {

    @Override
    public QueryResult getAllEntitiesOfType( FullQualifiedName entityTypeFqn ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public QueryResult getAllEntitiesOfType(
            FullQualifiedName entityTypeFqn,
            List<PropertyType> authorizedProperties ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public QueryResult getAllEntitiesOfEntitySet( FullQualifiedName entityFqn, String entitySetName ) {

        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public QueryResult getAllEntitiesOfEntitySet(
            FullQualifiedName entityFqn,
            String entitySetName,
            List<PropertyType> authorizedProperties ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public QueryResult getFilterEntities( LookupEntitiesRequest request ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public Void clustering( UUID linkedEntitySetId ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }
}