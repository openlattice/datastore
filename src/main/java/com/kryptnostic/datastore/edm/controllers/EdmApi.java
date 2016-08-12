package com.kryptnostic.datastore.edm.controllers;

import java.util.Set;
import java.util.UUID;

import com.google.common.base.Optional;
import com.kryptnostic.types.Container;
import com.kryptnostic.types.Namespace;
import com.kryptnostic.types.ObjectType;
import com.kryptnostic.types.PropertyType;
import com.kryptnostic.types.Schema;

public interface EdmApi {

    Iterable<Namespace> getSchemas();

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
            String namespace );

    /**
     * Creates a container if it doesn't already exist.
     * 
     * @param namespace Namespace for the container.
     * @param container Name of the container.
     * @return True if container was created, false if container already exists.
     */
    boolean postContainer( String namespace, Container container );

    /**
     * Creates or updates a container.
     * @param namespace Namespace for the container.
     * @param container Name of the container.
     */
    void putContainer(
            String namespace,
            Container container );

    void addObjectTypeToContainer(
            String namespace,
            String container,
            Set<String> objectTypes );

    void removeObjectTypeFromContainer(
            String namespace,
            String container,
            Set<String> objectTypes );

    /**
     * @param namespace Namespace for the object.
     * @param objectType Name of the container.
     * @return True if object type was created, false if container already exists.
     */
    boolean postObjectType( String namespace, ObjectType objectType );

    void putObjectType(
            String namespace,
            ObjectType typeInfo );

    void deleteObjectType(
            String namespace,
            String objectType );

    /**
     * Creates a property type if doesn't alreadsy exist.
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
}