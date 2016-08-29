package com.kryptnostic.datastore.odata;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.conductor.rpc.odata.Schema;
import com.kryptnostic.datastore.edm.controllers.EdmApi;

public class EntityDataModel {
    private final Set<String>       namespaces;
    private final Set<Schema>       schemas;
    private final Set<EntityType>   entityTypes;
    private final Set<PropertyType> propertyTypes;
    private final Set<EntitySet>    entitySets;

    public EntityDataModel(
            @JsonProperty( EdmApi.NAMESPACES ) Set<String> namespaces,
            @JsonProperty( EdmApi.SCHEMAS ) Set<Schema> schemas,
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
    public Set<Schema> getSchemas() {
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