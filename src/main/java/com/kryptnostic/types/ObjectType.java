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

    @ClusteringColumn
    private String      typanme;

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

    public Set<String> getKeys() {
        return keys;
    }

    public ObjectType setKeys( Set<String> keys ) {
        this.keys = keys;
        return this;
    }

}
