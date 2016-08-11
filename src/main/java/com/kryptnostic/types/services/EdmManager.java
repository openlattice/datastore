package com.kryptnostic.types.services;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

import com.kryptnostic.types.Container;
import com.kryptnostic.types.Namespace;
import com.kryptnostic.types.ObjectType;
import com.kryptnostic.types.PropertyType;
import com.kryptnostic.types.Schema;

public interface EdmManager {
    void createNamespace( String namespace, UUID aclId );

    void upsertNamespace( Namespace namespace );

    void deleteNamespace( Namespace namespaces );

    Iterable<Namespace> getNamespaces();

    Schema getSchema( String namespace );

    /**
     * Creates a container if does not exist. To upsert a container
     * 
     * @param namespace The namespace for the container.
     * @param container The name of the container.
     * @param aclId The aclId controlling access to the container.
     */
    void createContainer( String namespace, String container, UUID aclId );

    /**
     * Creates or updates a container.
     * 
     * @param namespace The namespace for the container.
     * @param container The name of the container.
     * @param aclId The aclId controlling access to the container.
     */
    void upsertContainer( Container container );

    void createObjectType( ObjectType propertyType );

    void createObjectType(
            String namespace,
            String type,
            String typename,
            Set<String> keys,
            Set<String> allowed );

    void upsertObjectType( ObjectType propertyType );

    void deleteObjectType( ObjectType objectType );

    void createPropertyType(
            String namespace,
            String type,
            String typename,
            EdmPrimitiveTypeKind datatype,
            long multiplicity );

    void upsertPropertyType( PropertyType propertyType );

    void deletePropertyType( PropertyType propertyType );

}