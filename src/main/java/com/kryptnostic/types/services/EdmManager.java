package com.kryptnostic.types.services;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

import com.kryptnostic.types.Container;
import com.kryptnostic.types.EntitySet;
import com.kryptnostic.types.Namespace;
import com.kryptnostic.types.EntityType;
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
     * @return True if the container was created and false otherwise.
     */
    boolean createContainer( String namespace, Container container );

    /**
     * Creates or updates a container.
     * 
     * @param namespace The namespace for the container.
     * @param container The name of the container.
     * @param aclId The aclId controlling access to the container.
     */
    void upsertContainer( Container container );
    
    boolean createEntitySet( String namespace, EntitySet entitySet );
    
    void upsertEntitySet( EntitySet entitySet );

    boolean createObjectType( EntityType objectType );

    void upsertObjectType( EntityType objectType );

    void deleteObjectType( EntityType objectType );

    boolean createPropertyType(
            String namespace,
            String type,
            String typename,
            EdmPrimitiveTypeKind datatype,
            long multiplicity );

    void upsertPropertyType( PropertyType propertyType );

    void deletePropertyType( PropertyType propertyType );

    void addEntityTypesToContainer( String namespace, String container, Set<String> objectTypes );

    void removeEntityTypesFromContainer( String namespace, String container, Set<String> objectTypes );

}