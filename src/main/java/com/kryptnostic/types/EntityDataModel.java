package com.kryptnostic.types;

import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.datastore.edm.controllers.EdmApi;

public class EntityDataModel {
    private final Set<String>         namespaces;
    private final Map<String, Schema> schemas;
    private final Set<EntityType>     entityTypes;
    private final Set<PropertyType>   propertyTypes;
    private final Set<EntitySet>      entitySets;

    public EntityDataModel(
            @JsonProperty( EdmApi.NAMESPACES ) Set<String> namespaces,
            @JsonProperty( EdmApi.SCHEMAS ) Map<String, Schema> schemas,
            @JsonProperty( EdmApi.ENTITY_TYPES ) Set<EntityType> entityTypes,
            @JsonProperty( EdmApi.PROPERTY_TYPES ) Set<PropertyType> propertyTypes,
            @JsonProperty( EdmApi.ENTITY_SETS ) Set<EntitySet> entitySets ) {
        this.namespaces = namespaces;
        this.schemas = schemas;
        this.entityTypes = entityTypes;
        this.propertyTypes = propertyTypes;
        this.entitySets = entitySets;
    }

    @JsonProperty( EdmApi.NAMESPACES )
    public Set<String> getNamespaces() {
        return namespaces;
    }

    @JsonProperty( EdmApi.SCHEMAS )
    public Map<String, Schema> getSchemas() {
        return schemas;
    }

    @JsonProperty( EdmApi.ENTITY_TYPES )
    public Set<EntityType> getEntityTypes() {
        return entityTypes;
    }

    @JsonProperty( EdmApi.PROPERTY_TYPES )
    public Set<PropertyType> getPropertyTypes() {
        return propertyTypes;
    }

    @JsonProperty( EdmApi.ENTITY_SETS )
    public Set<EntitySet> getEntitySets() {
        return entitySets;
    }

}