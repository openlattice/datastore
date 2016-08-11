package com.kryptnostic.types;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.kryptnostic.datastore.util.DatastoreConstants;

@Table(
    keyspace = DatastoreConstants.KEYSPACE,
    name = DatastoreConstants.PROPERTY_TYPES_TABLE )
public class PropertyType {
    @PartitionKey(
        value = 0 )
    private String namespace;
    @PartitionKey(
        value = 1 )
    private String type;

    @ClusteringColumn(
        value = 0 )
    private String typename;

    @Column(
        name = "datatype" )
    private EdmPrimitiveTypeKind datatype;

    @Column(
        name = "multiplicity" )
    public long    multiplicity;

    public String getNamespace() {
        return namespace;
    }

    public PropertyType setNamespace( String namespace ) {
        this.namespace = namespace;
        return this;
    }

    public String getType() {
        return type;
    }

    public PropertyType setType( String type ) {
        this.type = type;
        return this;
    }

    public String getTypename() {
        return typename;
    }

    public PropertyType setTypename( String typename ) {
        this.typename = typename;
        return this;
    }

    public EdmPrimitiveTypeKind getDatatype() {
        return datatype;
    }

    public PropertyType setDatatype( EdmPrimitiveTypeKind datatype ) {
        this.datatype = datatype;
        return this;
    }

    public long getMultiplicity() {
        return multiplicity;
    }

    public PropertyType setMultiplicity( long multiplicity ) {
        this.multiplicity = multiplicity;
        return this;
    }

    @Override
    public String toString() {
        return "ObjectType [namespace=" + namespace + ", type=" + type + ", typename=" + typename + ", datatype="
                + datatype
                + ", multiplicity=" + multiplicity + "]";
    }

}
