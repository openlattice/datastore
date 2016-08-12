package com.kryptnostic.types.services;

import java.util.List;
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
import com.kryptnostic.types.EntityType;
import com.kryptnostic.types.PropertyType;

@Accessor
public interface CassandraEdmStore {
    @Query( Queries.GET_ALL_ENTITY_TYPES_QUERY )
    public Result<EntityType> getObjectTypes();

    @Query( Queries.GET_ALL_ENTITY_TYPES_QUERY )
    public ListenableFuture<Result<EntityType>> getObjectTypesAsync();

    @Query( Queries.GET_ALL_PROPERTY_TYPES_FOR_ENTITY_TYPE )
    public Result<PropertyType> getPropertyTypesForObjectType(
            @Param( ParamNames.NAMESPACE ) String namespace,
            @Param( ParamNames.ENTITY_TYPE ) String objectType );

     @Query( Queries.GET_ALL_NAMESPACES )
     public Result<Namespace> getNamespaces( @Param( ParamNames.ACL_IDS ) List<UUID> aclIds );

    @Query( Queries.CREATE_ENTITY_TYPE_IF_NOT_EXISTS )
    public ResultSet createObjectTypeIfNotExists(
            String namespace,
            String type,
            String typename,
            Set<String> keys,
            Set<String> allowed );

    @Query( Queries.CREATE_PROPERTY_TYPE_IF_NOT_EXISTS )
    public ResultSet createPropertyTypeIfNotExists(
            String namespace,
            String type,
            String typename,
            EdmPrimitiveTypeKind datatype,
            long multiplicity );

    @Query( Queries.CREATE_NAMESPACE_IF_NOT_EXISTS )
    public ResultSet createNamespaceIfNotExists( String namespace, UUID aclId );

    /**
     * @param namespace
     * @param container
     * @param type the boe
     */
    @Query( Queries.CREATE_CONTAINER_IF_NOT_EXISTS )
    public ResultSet createContainerIfNotExists( String namespace, String container, Set<String> objectTypes );

    @Query( Queries.ADD_ENTITY_TYPES_TO_CONTAINER )
    public ResultSet addEntityTypesToContainer(
            @Param( ParamNames.NAMESPACE ) String namespace,
            @Param( ParamNames.CONTAINER ) String container,
            @Param( ParamNames.ENTITY_TYPES ) Set<String> objectType );

    @Query( Queries.REMOVE_OBJECT_TYPES_TO_CONTAINER )
    public ResultSet removeEntityTypesFromContainer(
            @Param( ParamNames.NAMESPACE ) String namespace,
            @Param( ParamNames.CONTAINER ) String container,
            @Param( ParamNames.ENTITY_TYPES ) Set<String> objectType );

}
