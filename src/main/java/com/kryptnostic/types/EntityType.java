package com.kryptnostic.types;

import java.util.Set;

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
    @PartitionKey(
        value = 1 )
    private String      type;

    @ClusteringColumn(
        value = 0 )
    private String      typename;

    @Column(
        name = "keys" )
    private Set<String> keys;

    @Column(
        name = "allowed" )
    public Set<String>  allowed;

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
        return keys;
    }

    public EntityType setKeys( Set<String> keys ) {
        this.keys = keys;
        return this;
    }

    public Set<String> getAllowed() {
        return allowed;
    }

    public EntityType setAllowed( Set<String> allowed ) {
        this.allowed = allowed;
        return this;
    }

    @Override
    public String toString() {
        return "ObjectType [namespace=" + namespace + ", type=" + type + ", typename=" + typename + ", keys=" + keys
                + ", allowed=" + allowed + "]";
    }
}
