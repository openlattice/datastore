package com.kryptnostic.types.services;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

import com.kryptnostic.types.Namespace;
import com.kryptnostic.types.ObjectType;
import com.kryptnostic.types.PropertyType;

public interface EdmManager {
    Iterable<Namespace> getNamespaces( Set<UUID> aclIds );

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

    void createNamespace( String namespace, UUID aclId );

    void upsertNamespace( Namespace namespace );

    void deleteNamespace( Namespace namespaces );

    void createObjectType( ObjectType propertyType );

}