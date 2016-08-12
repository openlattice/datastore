package com.kryptnostic.types;

import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.kryptnostic.datastore.edm.controllers.EdmApi;

public class Schema {
    private final Set<PropertyType>      propertyTypes;
    private final Set<EntityType>        objectTypes;
    private final Optional<UUID>         aclId;

    @JsonCreator
    public Schema(
            @JsonProperty( EdmApi.OBJECT_TYPES ) Set<EntityType> objectTypes,
            @JsonProperty( EdmApi.PROPERTY_TYPES ) Set<PropertyType> propertyTypes,
            @JsonProperty( EdmApi.ACL_ID ) Optional<UUID> aclId ) {
        this.propertyTypes = propertyTypes;
        this.objectTypes = objectTypes;
        this.aclId = aclId;
    }


    @JsonProperty( EdmApi.PROPERTY_TYPES )
    public Set<PropertyType> getPropertyTypes() {
        return propertyTypes;
    }

    @JsonProperty( EdmApi.OBJECT_TYPES )
    public Set<EntityType> getEntityTypes() {
        return objectTypes;
    }

    @JsonProperty( EdmApi.ACL_ID )
    public Optional<UUID> getAclId() {
        return aclId;
    }
}
