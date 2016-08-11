package com.kryptnostic.types.services;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

import com.kryptnostic.types.Namespace;
import com.kryptnostic.types.ObjectType;
import com.kryptnostic.types.PropertyType;

public interface EdmManager {
    void createNamespace( String namespace, UUID aclId );
    
    void upsertNamespace( Namespace namespace );
    
    void deleteNamespace( Namespace namespaces );
    
    Iterable<Namespace> getNamespaces( Set<UUID> aclIds );

    void createContainer( String namespace, String container, UUID or );
    
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