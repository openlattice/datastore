package com.kryptnostic.datastore.edm.controllers;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Optional;
import com.kryptnostic.types.EntitySet;
import com.kryptnostic.types.EntityType;
import com.kryptnostic.types.PropertyType;
import com.kryptnostic.types.Schema;
import com.kryptnostic.types.SchemaMetadata;

import retrofit.client.Response;
import retrofit.http.Body;
import retrofit.http.DELETE;
import retrofit.http.GET;
import retrofit.http.POST;
import retrofit.http.PUT;
import retrofit.http.Path;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public interface EdmApi {
    String ALIAS                   = "alias";
    String ACL_ID                  = "aclId";
    String CONTAINER               = "container";
    String CONTAINERS              = "containers";
    String NAME                    = "name";
    String NAMESPACE               = "namespace";
    String OBJECT_TYPES            = "objectTypes";
    String PROPERTY_TYPES          = "propertyTypes";
    String SCHEMA                  = "schema";

    // {namespace}/{schema_name}/{class}/{FQN}/{FQN}
    /*
     * kryptnostic/primary/entity/type kryptnostic/primary/entity/set/{
     */
    String SCHEMA_BASE_PATH        = "/";
    String ENTITY_SETS_BASE_PATH   = "/entity/set";
    String ENTITY_TYPE_BASE_PATH   = "/entity/type";
    String PROPERTY_TYPE_BASE_PATH = "/property/type";
    String NAMESPACE_PATH          = "/{" + NAMESPACE + "}";
    String NAME_PATH               = "/{" + NAME + "}";

    /**
     * Gets all schemas available to the calling user.
     * 
     * @return An iterable containing all the schemas available to the calling user.
     */
    @GET( SCHEMA_BASE_PATH )
    Iterable<SchemaMetadata> getSchemas();

    /**
     * Creates a schema.
     * 
     * @param namespace The namespace for the schema.
     * @param aclId The id of the ACL controlling access to the schema.
     */
    @PUT( SCHEMA_BASE_PATH + NAMESPACE_PATH )
    Response putSchema(
            @Path( NAMESPACE ) String namespace,
            @Body Optional<UUID> aclId );

    /**
     * Retrieves the schema for a corresponding namespace
     * 
     * @param namespace
     * @return The schema for the namespace specified by namespace.
     */
    @GET( SCHEMA_BASE_PATH + NAMESPACE_PATH )
    Schema getSchema(
            String namespace,
            String name );

    /**
     * Retrieves all schemas associated with a given namespace and accessible by the caller.
     * 
     * @param namespace The namespace for which to retrieve all accessible schemas.
     * @return All accessible schemas in the provided namespace.
     */
    @GET( SCHEMA_BASE_PATH + NAMESPACE_PATH )
    Iterable<Schema> getSchemasInNamespace( String namespace );

    /**
     * @param namespace
     * @param name
     * @param entityTypes
     * @return
     */
    @PUT( SCHEMA_BASE_PATH + NAMESPACE_PATH + NAME_PATH )
    Response addEntityTypeToSchema(
            @Path( NAMESPACE ) String namespace,
            @Path( NAME ) String name,
            @Body Set<String> entityTypes );

    @DELETE( SCHEMA_BASE_PATH + NAMESPACE_PATH + NAME_PATH )
    void removeEntityTypeFromSchema(
            @Path( NAMESPACE ) String namespace,
            @Path( NAME ) String name,
            @Body Set<String> entityTypes );

    /**
     * Creates multiple entity sets, if they do not exist.
     * 
     * @param entitySets The entity sets to create.
     * @return A map of describing whether or not each entity set was created.
     * 
     */
    @POST( ENTITY_SETS_BASE_PATH )
    Map<String, Boolean> postEntitySets( @Body Set<EntitySet> entitySets );

    /**
     * Creates or updates multiple entity sets.
     * 
     * @param entitySets The entity sets to create.
     * @return A map of describing whether or not posted entity sets were created or updated.
     * 
     */
    @POST( ENTITY_SETS_BASE_PATH )
    Response putEntitySets( @Body Set<EntitySet> entitySets );

    /**
     * @param namespace Namespace for the object.
     * @param objectType Name of the container.
     * @return True if object type was created, false if container already exists.
     */
    boolean postEntityType( String namespace, EntityType entityType );

    @PUT( ENTITY_TYPE_BASE_PATH )
    Response putEntityType( EntityType entityType );

    @DELETE( ENTITY_TYPE_BASE_PATH + NAMESPACE_PATH + NAME_PATH )
    void deleteEntityType( String namespace, String entityTypeName );

    /**
     * Creates a property type if doesn't alreadsy exist.
     * 
     * @param namespace Namespace for the object.
     * @param propertyType Name of the property type.
     * @return True if property type was created, false if container already exists.
     */
    boolean postPropertyType(
            String namespace,
            PropertyType propertyType );

    void putPropertyType(
            String namespace,
            PropertyType typeInfo );

    void deletePropertyType(
            String namespace,
            String propertyType );

}