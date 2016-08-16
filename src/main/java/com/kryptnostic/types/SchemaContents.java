package com.kryptnostic.types;

import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.kryptnostic.datastore.edm.controllers.EdmApi;
import com.kryptnostic.datastore.util.UUIDs.ACLs;

public class SchemaContents {
    private final Set<PropertyType> propertyTypes;
    private final Set<EntityType>   objectTypes;
    private final UUID              aclId;

    @JsonCreator
    public SchemaContents(
            @JsonProperty( EdmApi.ENTITY_TYPES ) Set<EntityType> objectTypes,
            @JsonProperty( EdmApi.PROPERTY_TYPES ) Set<PropertyType> propertyTypes,
            @JsonProperty( EdmApi.ACL_ID ) Optional<UUID> aclId ) {
        this.propertyTypes = propertyTypes;
        this.objectTypes = objectTypes;
        this.aclId = aclId.or( ACLs.EVERYONE_ACL );
    }

    @JsonProperty( EdmApi.PROPERTY_TYPES )
    public Set<PropertyType> getPropertyTypes() {
        return propertyTypes;
    }

    @JsonProperty( EdmApi.ENTITY_TYPES )
    public Set<EntityType> getEntityTypes() {
        return objectTypes;
    }

    @JsonProperty( EdmApi.ACL_ID )
    public UUID getAclId() {
        return aclId;
    }
}
