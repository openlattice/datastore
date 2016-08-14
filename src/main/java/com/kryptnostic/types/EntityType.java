package com.kryptnostic.types;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.kryptnostic.datastore.util.DatastoreConstants;

@Table(
    keyspace = DatastoreConstants.KEYSPACE,
    name = DatastoreConstants.ENTITY_TYPES_TABLE )
public class EntityType {
    @PartitionKey(
        value = 0 )
    private String      namespace;
    @ClusteringColumn(
        value = 0 )
    private String      type;
    @Column(
        name = "typename" )
    private String      typename;

    @Column(
        name = "key" )
    private Set<String> key;

    @Column(
        name = "properties" )
    public Set<FullQualifiedName>  properties;

    public String getNamespace() {
        return namespace;
    }

    public EntityType setNamespace( String namespace ) {
        this.namespace = namespace;
        return this;
    }

    public String getType() {
        return type;
    }

    public EntityType setType( String type ) {
        this.type = type;
        return this;
    }

    public String getTypename() {
        return typename;
    }

    public EntityType setTypename( String typename ) {
        this.typename = typename;
        return this;
    }

    public Set<String> getKey() {
        return key;
    }

    public EntityType setKey( Set<String> key ) {
        this.key = key;
        return this;
    }

    public Set<FullQualifiedName> getProperties() {
        return properties;
    }

    public EntityType setProperties( Set<FullQualifiedName> properties ) {
        this.properties = properties;
        return this;
    }

    @Override
    public String toString() {
        return "ObjectType [namespace=" + namespace + ", type=" + type + ", typename=" + typename + ", key=" + key
                + ", allowed=" + properties + "]";
    }
}
