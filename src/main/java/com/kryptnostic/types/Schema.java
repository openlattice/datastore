package com.kryptnostic.types;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;

import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.datastore.edm.controllers.EdmApi;
import com.kryptnostic.datastore.util.DatastoreConstants;
import com.kryptnostic.datastore.util.UUIDs.ACLs;

import jersey.repackaged.com.google.common.base.Preconditions;
import jersey.repackaged.com.google.common.collect.Sets;

/**
 * This class roughly corresponds to {@link CsdlSchema} and is annotated for use by the {@link MappingManager} to R/W
 * from Cassandra.
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 * 
 */
@Table(
    keyspace = DatastoreConstants.KEYSPACE,
    name = DatastoreConstants.SCHEMAS_TABLE )
public class Schema {
    @PartitionKey(
        value = 0 )
    private UUID                    aclId;

    @ClusteringColumn(
        value = 0 )
    private String                  namespace;

    @ClusteringColumn(
        value = 1 )
    private String                  name;

    @Column(
        name = "entityTypeNames" )
    private Set<FullQualifiedName>  entityTypeFqns;

    @Transient
    private final Set<PropertyType> propertyTypes;

    @Transient
    private final Set<EntityType>   entityTypes;

    public Schema() {
        this( ImmutableSet.of(), ImmutableSet.of() );
    }

    public Schema( Set<EntityType> entityTypes, Set<PropertyType> propertyTypes ) {
        this.entityTypes = Sets.newHashSet( entityTypes );
        this.propertyTypes = Sets.newHashSet( propertyTypes );

        setEntityTypeNames( entityTypes.stream()
                .map( entityType -> new FullQualifiedName( entityType.getNamespace(), entityType.getType() ) )
                .collect( Collectors.toSet() ) );
    }

    @JsonProperty( EdmApi.NAMESPACE )
    public String getNamespace() {
        return namespace;
    }

    public Schema setNamespace( String namespace ) {
        this.namespace = namespace;
        return this;
    }

    @JsonProperty( EdmApi.ACL_ID )
    public UUID getAclId() {
        return aclId;
    }

    public Schema setAclId( UUID aclId ) {
        this.aclId = aclId;
        return this;
    }

    @JsonProperty( EdmApi.NAME )
    public String getName() {
        return name;
    }

    public Schema setName( String name ) {
        this.name = name;
        return this;
    }

    public Set<FullQualifiedName> getEntityTypeFqns() {
        return entityTypeFqns;
    }

    public Schema setEntityTypeNames( Set<FullQualifiedName> entityTypeFqns ) {
        this.entityTypeFqns = entityTypeFqns;
        return this;
    }

    public Set<EntityType> getEntityTypes() {
        return entityTypes;
    }

    public Set<PropertyType> getPropertyTypes() {
        return propertyTypes;
    }

    public void addEntityTypes( Set<EntityType> entityTypes ) {
        this.entityTypes.addAll( entityTypes );
        // Need to sync entity type names
        entityTypes.forEach( entityType -> entityTypeFqns
                .add( new FullQualifiedName( entityType.getNamespace(), entityType.getType() ) ) );
    }

    public void addPropertyTypes( Set<PropertyType> propertyTypes ) {
        this.propertyTypes.addAll( propertyTypes );
    }

    @Override
    public String toString() {
        return "Schema [aclId=" + aclId + ", namespace=" + namespace + ", name=" + name + ", entityTypeNames="
                + entityTypeFqns + ", propertyTypes=" + propertyTypes + ", entityTypes=" + entityTypes + "]";
    }

    @JsonCreator
    public static Schema deserializeSchema(
            Optional<UUID> aclId,
            String namespace,
            String name,
            Optional<Set<FullQualifiedName>> entityTypeNames,
            Optional<Set<EntityType>> entityTypes,
            Optional<Set<PropertyType>> propertyTypes ) {

        Preconditions.checkArgument(
                ( entityTypes.isPresent() && !entityTypeNames.isPresent() ) || !entityTypes.isPresent() );

        Schema schema = new Schema( entityTypes.or( ImmutableSet.of() ), propertyTypes.or( ImmutableSet.of() ) )
                .setAclId( aclId.or( ACLs.EVERYONE_ACL ) )
                .setNamespace( namespace )
                .setName( name );

        schema.setEntityTypeNames( entityTypeNames.or( schema.getEntityTypeFqns() ) );

        return schema;
    }
}
