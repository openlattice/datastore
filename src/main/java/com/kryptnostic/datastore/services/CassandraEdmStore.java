package com.kryptnostic.datastore.services;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import com.google.common.util.concurrent.ListenableFuture;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.conductor.rpc.odata.Schema;
import com.kryptnostic.datastore.cassandra.Queries;
import com.kryptnostic.datastore.cassandra.Queries.ParamNames;

@Accessor
public interface CassandraEdmStore {
    @Query( Queries.GET_ALL_SCHEMAS_IN_NAMESPACE )
    public Result<Schema> getSchemas(
            @Param( ParamNames.NAMESPACE ) String namespace,
            @Param( ParamNames.ACL_IDS ) List<UUID> everyoneAcl );

    @Query( Queries.GET_ALL_NAMESPACES )
    public Result<Schema> getSchemas( @Param( ParamNames.ACL_IDS ) List<UUID> aclIds );

    @Query( Queries.GET_ALL_ENTITY_TYPES_QUERY )
    public Result<EntityType> getEntityTypes();

    @Query( Queries.GET_ALL_ENTITY_TYPES_QUERY )
    public ListenableFuture<Result<EntityType>> getObjectTypesAsync();

    @Query( Queries.GET_ALL_PROPERTY_TYPES_IN_NAMESPACE )
    public Result<PropertyType> getPropertyTypesInNamespace( String namespace );

    @Query( Queries.CREATE_ENTITY_TYPE_IF_NOT_EXISTS )
    public ResultSet createEntityTypeIfNotExists(
            String namespace,
            String type,
            String typename,
            Set<FullQualifiedName> key,
            Set<FullQualifiedName> properties );

    @Query( Queries.CREATE_PROPERTY_TYPE_IF_NOT_EXISTS )
    public ResultSet createPropertyTypeIfNotExists(
            String namespace,
            String type,
            String typename,
            EdmPrimitiveTypeKind datatype,
            long multiplicity );

    @Query( Queries.CREATE_SCHEMA_IF_NOT_EXISTS )
    public ResultSet createSchemaIfNotExists(
            String namespace,
            String name,
            UUID aclId,
            Set<FullQualifiedName> entityTypes );

    @Query( Queries.ADD_ENTITY_TYPES_TO_SCHEMA )
    public ResultSet addEntityTypesToContainer(
            @Param( ParamNames.NAMESPACE ) String namespace,
            @Param( ParamNames.ACL_ID ) UUID aclId,
            @Param( ParamNames.NAME ) String name,
            @Param( ParamNames.ENTITY_TYPES ) Set<FullQualifiedName> entityTypes );

    @Query( Queries.REMOVE_ENTITY_TYPES_FROM_SCHEMA )
    public ResultSet removeEntityTypesFromContainer(
            @Param( ParamNames.NAMESPACE ) String namespace,
            @Param( ParamNames.ACL_ID ) UUID aclId,
            @Param( ParamNames.NAME ) String name,
            @Param( ParamNames.ENTITY_TYPES ) Set<FullQualifiedName> entityTypes );

    @Query( Queries.CREATE_ENTITY_SET_IF_NOT_EXISTS )
    public ResultSet createEntitySet( FullQualifiedName type, String name, String title );

    @Query( Queries.GET_ENTITY_SET_BY_NAME )
    public EntitySet getEntitySet( String name );

    @Query( Queries.GET_ALL_ENTITY_SETS )
    public Result<EntitySet> getEntitySets();

    @Query( Queries.COUNT_ENTITY_SET )
    public ResultSet countEntitySet( FullQualifiedName type, String name );

}
