package com.kryptnostic.types.services;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import com.google.common.util.concurrent.ListenableFuture;
import com.kryptnostic.datastore.util.DatastoreConstants.Queries;
import com.kryptnostic.datastore.util.DatastoreConstants.Queries.ParamNames;
import com.kryptnostic.types.Namespace;
import com.kryptnostic.types.ObjectType;

@Accessor
public interface CassandraEdmStore {
    @Query( Queries.GET_ALL_OBJECT_TYPES_QUERY )
    public Result<ObjectType> getObjectTypes();

    @Query( Queries.GET_ALL_OBJECT_TYPES_QUERY )
    public ListenableFuture<Result<ObjectType>> getObjectTypesAsync();

    // @Query( Queries.GET_ALL_PROPERTY_TYPES_FOR_OBJECT )
    // public Result<PropertyType> getPropertyTypesForObjectId( @Param( ParamNames.OBJECT_ID ) UUID objectId );

    @Query( Queries.GET_ALL_NAMESPACES )
    public Result<Namespace> getNamespaces( @Param( ParamNames.ACL_IDS ) Set<UUID> aclIds );
    
    @Query( "INSERT INTO sparks.object_types (namespace, type, typename, keys, allowed) VALUES (?,?,?,?,?) IF NOT EXISTS" )
    public ResultSet createObjectTypeIfNotExists( 
            String namespace,
            String type,
            String typename,
            Set<String> keys,
            Set<String> allowed) ;

    @Query( "INSERT INTO sparks.property_types (namespace, type, typename, datatype, multiplicity) VALUES (?,?,?,?,?) IF NOT EXISTS" )
    public ResultSet createPropertyTypeIfNotExists(
            String namespace,
            String type,
            String typename,
            EdmPrimitiveTypeKind datatype,
            long multiplicity );

}
