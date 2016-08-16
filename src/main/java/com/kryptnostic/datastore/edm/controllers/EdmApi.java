package com.kryptnostic.datastore.edm.controllers;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.google.common.base.Optional;
import com.kryptnostic.types.EntityDataModel;
import com.kryptnostic.types.EntitySet;
import com.kryptnostic.types.EntityType;
import com.kryptnostic.types.GetSchemasRequest;
import com.kryptnostic.types.PropertyType;
import com.kryptnostic.types.Schema;

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
    String LOAD_DETAILS            = "loadDetails";
    String NAME                    = "name";
    String NAMESPACE               = "namespace";
    String NAMESPACES              = "namespaces";
    String ENTITY_SETS             = "entitySets";
    String ENTITY_TYPES            = "objectTypes";
    String PROPERTY_TYPES          = "propertyTypes";
    String SCHEMA                  = "schema";
    String SCHEMAS                 = "schemas;";

    // {namespace}/{schema_name}/{class}/{FQN}/{FQN}
    /*
     * /entity/type/{namespace}/{name} /entity/set/{namespace}/{name} /schema/{namespace}/{name}
     * /property/{namespace}/{name}
     */
    String SCHEMA_BASE_PATH        = "/schema";
    String ENTITY_SETS_BASE_PATH   = "/entity/set";
    String ENTITY_TYPE_BASE_PATH   = "/entity/type";
    String PROPERTY_TYPE_BASE_PATH = "/property/type";
    String NAMESPACE_PATH          = "/{" + NAMESPACE + "}";
    String NAME_PATH               = "/{" + NAME + "}";

    @GET( "/" )
    EntityDataModel getEntityDataModel();

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
     * Retrieves schemas matching the namespace provided in the {@code request} parameter. If no namespace is specified
     * then all schemas will be returned.
     * 
     * The level of type detail returned by the server is determined those provided in the {@code typeDetails } field of
     * the request. If no type details are specified then the server return all type details.
     * 
     * The server will only return schemas that the calling user is authorized to see.
     * 
     * @param request The request options to use when filtering schemas.
     * @return An iterable of Schema objects.
     */
    @POST( SCHEMA_BASE_PATH )
    Iterable<Schema> getSchemas( @Body GetSchemasRequest request );

    /**
     * Gets all schemas available to the calling user.
     * 
     * @return An iterable containing all the schemas available to the calling user.
     */
    @GET( SCHEMA_BASE_PATH )
    Iterable<Schema> getSchemas();

    /**
     * Retrieves all schemas associated with a given namespace and accessible by the caller.
     * 
     * @param namespace The namespace for which to retrieve all accessible schemas.
     * @return All accessible schemas in the provided namespace.
     */
    @GET( SCHEMA_BASE_PATH + NAMESPACE_PATH )
    Iterable<Schema> getSchemasInNamespace( String namespace );

    /**
     * Retrieves the schema contents for a corresponding namespace
     * 
     * @param namespace
     * @return The schema for the namespace specified by namespace.
     */
    @GET( SCHEMA_BASE_PATH + NAMESPACE_PATH + NAME_PATH )
    Schema getSchemaContents(
            @Path( NAMESPACE ) String namespace,
            @Path( NAME ) String name );

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
            @Body Set<FullQualifiedName> entityTypes );

    @DELETE( SCHEMA_BASE_PATH + NAMESPACE_PATH + NAME_PATH )
    Response removeEntityTypeFromSchema(
            @Path( NAMESPACE ) String namespace,
            @Path( NAME ) String name,
            @Body Set<FullQualifiedName> entityTypes );

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
    @PUT( ENTITY_SETS_BASE_PATH )
    Response putEntitySets( @Body Set<EntitySet> entitySets );

    /**
     * @param namespace Namespace for the object.
     * @param objectType Name of the container.
     * @return True if object type was created, false if container already exists.
     */
    @POST( ENTITY_TYPE_BASE_PATH )
    boolean postEntityType( @Body EntityType entityType );

    @PUT( ENTITY_TYPE_BASE_PATH )
    Response putEntityType( @Body EntityType entityType );

    @GET( ENTITY_TYPE_BASE_PATH + NAMESPACE_PATH + NAME_PATH )
    EntityType getEntityType( String namespace, String entityTypeName );

    @DELETE( ENTITY_TYPE_BASE_PATH + NAMESPACE_PATH + NAME_PATH )
    Response deleteEntityType( String namespace, String entityTypeName );

    /**
     * Creates a property type if doesn't alreadsy exist.
     * 
     * @param namespace Namespace for the object.
     * @param propertyType Name of the property type.
     * @return True if property type was created, false if container already exists.
     */
    @POST( PROPERTY_TYPE_BASE_PATH )
    boolean postPropertyType(
            PropertyType propertyType );

    /**
     * @param typeInfo
     * @return An HTTP 200 response with an empty body, if successful. Otherwise, an appropriate HttpStatus code and
     *         potential error message.
     */
    @PUT( PROPERTY_TYPE_BASE_PATH )
    Response putPropertyType( PropertyType typeInfo );

    @DELETE( PROPERTY_TYPE_BASE_PATH + NAMESPACE_PATH + NAME_PATH )
    Response deletePropertyType( String namespace, String name );

}