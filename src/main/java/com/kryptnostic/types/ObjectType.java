package com.kryptnostic.types;

import java.util.Set;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(
    keyspace = "sparks",
    name = "object_types" )
public class ObjectType {
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

    public ObjectType setNamespace( String namespace ) {
        this.namespace = namespace;
        return this;
    }

    public String getType() {
        return type;
    }

    public ObjectType setType( String type ) {
        this.type = type;
        return this;
    }

    public String getTypename() {
        return typename;
    }

    public ObjectType setTypename( String typename ) {
        this.typename = typename;
        return this;
    }

    public Set<String> getKeys() {
        return keys;
    }

    public ObjectType setKeys( Set<String> keys ) {
        this.keys = keys;
        return this;
    }

    public Set<String> getAllowed() {
        return allowed;
    }

    public ObjectType setAllowed( Set<String> allowed ) {
        this.allowed = allowed;
        return this;
    }

    @Override
    public String toString() {
        return "ObjectType [namespace=" + namespace + ", type=" + type + ", typename=" + typename + ", keys=" + keys
                + ", allowed=" + allowed + "]";
    }
}
