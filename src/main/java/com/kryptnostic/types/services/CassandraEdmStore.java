package com.kryptnostic.types.services;

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
import com.kryptnostic.datastore.util.DatastoreConstants.Queries;
import com.kryptnostic.datastore.util.DatastoreConstants.Queries.ParamNames;
import com.kryptnostic.types.EntitySet;
import com.kryptnostic.types.EntityType;
import com.kryptnostic.types.PropertyType;
import com.kryptnostic.types.Schema;

@Accessor
public interface CassandraEdmStore {
    @Query( Queries.GET_ALL_SCHEMAS_IN_NAMESPACE )
    public Result<Schema> getSchemas(
            @Param( ParamNames.NAMESPACE ) String namespace,
            @Param( ParamNames.ACL_IDS ) List<UUID> everyoneAcl );

    @Query( Queries.GET_ALL_NAMESPACES )
    public Result<Schema> getSchemaMetadata( @Param( ParamNames.ACL_IDS ) List<UUID> aclIds );

    @Query( Queries.GET_ALL_ENTITY_TYPES_QUERY )
    public Result<EntityType> getEntityTypes();

    @Query( Queries.GET_ALL_ENTITY_TYPES_QUERY )
    public ListenableFuture<Result<EntityType>> getObjectTypesAsync();

    @Query( Queries.GET_ALL_PROPERTY_TYPES_IN_NAMESPACE )
    public Result<PropertyType> getPropertyTypesInNamespace( String namespace );
    
    @Query( Queries.GET_ALL_PROPERTY_TYPES_FOR_ENTITY_TYPE )
    public Result<PropertyType> getPropertyTypesForEntityType(
            @Param( ParamNames.NAMESPACE ) String namespace,
            @Param( ParamNames.ENTITY_TYPE ) String objectType );

    @Query( Queries.CREATE_ENTITY_TYPE_IF_NOT_EXISTS )
    public ResultSet createEntityTypeIfNotExists(
            String namespace,
            String type,
            String typename,
            Set<String> key,
            Set<FullQualifiedName> properties );

    @Query( Queries.CREATE_PROPERTY_TYPE_IF_NOT_EXISTS )
    public ResultSet createPropertyTypeIfNotExists(
            String namespace,
            String type,
            String typename,
            EdmPrimitiveTypeKind datatype,
            long multiplicity );

    @Query( Queries.CREATE_SCHEMA_IF_NOT_EXISTS )
    public ResultSet createSchemaIfNotExists( String namespace, String name, UUID aclId, Set<String> entityTypes );

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

}
