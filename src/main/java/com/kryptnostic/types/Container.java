package com.kryptnostic.types;

import java.util.Set;

import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainer;

import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.kryptnostic.datastore.edm.controllers.EdmApi;
import com.kryptnostic.datastore.util.DatastoreConstants;

import jersey.repackaged.com.google.common.collect.ImmutableSet;

/**
 * This class roughly correspond to {@link CsdlEntityContainer} and is annotated for use by the {@link MappingManager}
 * to R/W from Cassandra.
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 *
 */
@Table(
    keyspace = DatastoreConstants.KEYSPACE,
    name = DatastoreConstants.CONTAINERS_TABLE )
public class Container {
    @PartitionKey(
        value = 0 )
    private String      namespace;
    @ClusteringColumn(
        value = 0 )
    private String      container;
    private Set<String> objectTypes;

    @JsonProperty( EdmApi.NAMESPACE )
    public String getNamespace() {
        return namespace;
    }

    public Container setNamespace( String namespace ) {
        this.namespace = namespace;
        return this;
    }

    @JsonProperty( EdmApi.CONTAINER )
    public String getContainer() {
        return container;
    }

    public Container setContainer( String container ) {
        this.container = container;
        return this;
    }

    @JsonProperty( EdmApi.OBJECT_TYPES )
    public Set<String> getObjectTypes() {
        return objectTypes;
    }

    public Container setObjectTypes( Set<String> objectTypes ) {
        this.objectTypes = objectTypes;
        return this;
    }

    @JsonCreator
    public static Container newContainer(
            @JsonProperty( EdmApi.NAMESPACE ) String namespace,
            @JsonProperty( EdmApi.CONTAINER ) String container,
            @JsonProperty( EdmApi.ACL_ID ) Optional<Set<String>> objectTypes ) {
        return new Container().setNamespace( namespace ).setContainer( container )
                .setObjectTypes( objectTypes.or( ImmutableSet.of() ) );
    }
}
