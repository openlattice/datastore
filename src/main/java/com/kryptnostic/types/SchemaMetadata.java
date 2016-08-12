package com.kryptnostic.types;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.provider.CsdlSchema;

import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.kryptnostic.datastore.edm.controllers.EdmApi;
import com.kryptnostic.datastore.util.DatastoreConstants;

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
public class SchemaMetadata {
    @PartitionKey(
        value = 0 )
    private UUID        aclId;

    @ClusteringColumn(
        value = 0 )
    private String      namespace;

    @ClusteringColumn(
        value = 1 )
    private String      name;

    @Column(
        name = "entityTypes" )
    private Set<String> entityTypes;

    @JsonProperty( EdmApi.NAMESPACE )
    public String getNamespace() {
        return namespace;
    }

    public SchemaMetadata setNamespace( String namespace ) {
        this.namespace = namespace;
        return this;
    }

    @JsonProperty( EdmApi.ACL_ID )
    public UUID getAclId() {
        return aclId;
    }

    public SchemaMetadata setAclId( UUID aclId ) {
        this.aclId = aclId;
        return this;
    }

    @JsonProperty( EdmApi.NAME )
    public String getName() {
        return name;
    }

    public SchemaMetadata setName( String name ) {
        this.name = name;
        return this;
    }

    public Set<String> getEntityTypes() {
        return entityTypes;
    }

    public SchemaMetadata setEntityTypes( Set<String> entityTypes ) {
        this.entityTypes = entityTypes;
        return this;
    }

    @Override
    public String toString() {
        return "SchemaMetadata [namespace=" + namespace + ", aclId=" + aclId + ", name=" + name + ", entityTypes="
                + entityTypes + "]";
    }

    // TODO: Add JsonCreator

}
