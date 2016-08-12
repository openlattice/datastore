package com.kryptnostic.datastore.edm.controllers;

import java.util.Set;
import java.util.UUID;

import com.google.common.base.Optional;
import com.kryptnostic.types.EntityType;
import com.kryptnostic.types.PropertyType;
import com.kryptnostic.types.Schema;
import com.kryptnostic.types.SchemaMetadata;

public interface EdmApi {

    Iterable<SchemaMetadata> getSchemas();

    /**
     * Creates a schema.
     * 
     * @param namespace The namespace for the schema.
     * @param aclId The id of the ACL controlling access to the schema.
     */
    void putSchema(
            String namespace,
            Optional<UUID> aclId );

    /**
     * Retrieves the schema for a corresponding namespace
     * 
     * @param namespace
     * @return The schema for the namespace specified by namespace.
     */
    Schema getSchema(
            String namespace,
            String name );

    void addEntityTypeToSchema(
            String namespace,
            String name,
            Set<String> objectTypes );

    void removeEntityTypeFromSchema(
            String namespace,
            String name,
            Set<String> objectTypes );

    /**
     * @param namespace Namespace for the object.
     * @param objectType Name of the container.
     * @return True if object type was created, false if container already exists.
     */
    boolean postEntityType( String namespace, EntityType objectType );

    void putEntityType(
            String namespace,
            EntityType typeInfo );

    void deleteEntityType(
            String namespace,
            String entityType );

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

    String CONTAINERS     = "containers";
    String PROPERTY_TYPES = "propertyTypes";
    String OBJECT_TYPES   = "objectTypes";
    String ACL_ID         = "aclId";
    String CONTAINER      = "container";
    String NAMESPACE      = "namespace";
    String NAME           = "name";
}