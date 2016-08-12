package com.kryptnostic.types;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.kryptnostic.datastore.util.DatastoreConstants;

@Table(
        keyspace = DatastoreConstants.KEYSPACE,
        name = DatastoreConstants.ENTITY_SETS_TABLE )
public class EntitySet {
    @PartitionKey(
        value = 0 )
    private String namespace;
    @PartitionKey(
        value = 1 )
    private String container;
    @ClusteringColumn(
        value = 0 )
    private String name;

    @Column(
        name = "type" )
    private String type;

    public String getName() {
        return name;
    }

    public EntitySet setName( String name ) {
        this.name = name;
        return this;
    }

    public String getNamespace() {
        return namespace;
    }

    public EntitySet setNamespace( String namespace ) {
        this.namespace = namespace;
        return this;
    }

    public String getContainer() {
        return container;
    }

    public EntitySet setContainer( String container ) {
        this.container = container;
        return this;
    }

    public String getType() {
        return type;
    }

    public EntitySet setType( String type ) {
        this.type = type;
        return this;
    }

}
