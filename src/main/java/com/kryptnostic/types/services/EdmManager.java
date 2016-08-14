package com.kryptnostic.types.services;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.kryptnostic.types.EntitySet;
import com.kryptnostic.types.EntityType;
import com.kryptnostic.types.PropertyType;
import com.kryptnostic.types.Schema;
import com.kryptnostic.types.SchemaMetadata;

public interface EdmManager {
    boolean createSchema( String namespace, String name, UUID aclId, Set<String> entityTypes );

    void upsertSchema( SchemaMetadata namespace );

    void deleteSchema( SchemaMetadata namespaces );

    Iterable<SchemaMetadata> getSchemaMetadata();

    Schema getSchema( String namespace, String name );

    boolean createEntitySet( String namespace, String name, String type );
    boolean createEntitySet( EntitySet entitySet );
    
    void upsertEntitySet( EntitySet entitySet );

    EntitySet getEntitySet( String namespace, String name );
    Iterable<EntitySet> getEntitySets();
    
    void deleteEntitySet( EntitySet entitySet );
    
    boolean createEntityType(
            String namespace,
            String type,
            String typename,
            Set<String> key,
            Set<FullQualifiedName> properties );
    boolean createEntityType( EntityType objectType );

    void upsertEntityType( EntityType objectType );

    EntityType getEntityType( String namespace, String name );

    void deleteEntityType( EntityType objectType );

    void addEntityTypesToSchema( String namespace, String name, Set<String> objectTypes );
    
    void removeEntityTypesFromSchema( String namespace, String name, Set<String> objectTypes );

    boolean createPropertyType(
            String namespace,
            String type,
            String typename,
            EdmPrimitiveTypeKind datatype,
            long multiplicity );

    void upsertPropertyType( PropertyType propertyType );

    void deletePropertyType( PropertyType propertyType );

    PropertyType getPropertyType(FullQualifiedName prop);

}