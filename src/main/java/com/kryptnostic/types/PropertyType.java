package com.kryptnostic.types;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import com.kryptnostic.datastore.util.DatastoreConstants;

@Table(
    keyspace = DatastoreConstants.KEYSPACE,
    name = DatastoreConstants.PROPERTY_TYPES_TABLE )
public class PropertyType extends PropertyTypeKey {

    @Column(
        name = "typename" )
    protected String typename;

    @Column(
        name = "multiplicity" )
    public long      multiplicity;

    @Override
    public PropertyType setNamespace( String namespace ) {
        this.namespace = namespace;
        return this;
    }
    
    public String getTypename() {
        return typename;
    }

    @Override
    public PropertyType setType( String type ) {
        this.type = type;
        return this;
    }

    public PropertyType setTypename( String typename ) {
        this.typename = typename;
        return this;
    }

    @Override
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

}
