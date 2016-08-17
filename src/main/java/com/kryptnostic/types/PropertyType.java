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

    @Override
    public String toString() {
        return "PropertyType [typename=" + typename + ", multiplicity=" + multiplicity + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (int) ( multiplicity ^ ( multiplicity >>> 32 ) );
        result = prime * result + ( ( typename == null ) ? 0 : typename.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( !super.equals( obj ) ) {
            return false;
        }
        if ( !( obj instanceof PropertyType ) ) {
            return false;
        }
        PropertyType other = (PropertyType) obj;
        if ( multiplicity != other.multiplicity ) {
            return false;
        }
        if ( typename == null ) {
            if ( other.typename != null ) {
                return false;
            }
        } else if ( !typename.equals( other.typename ) ) {
            return false;
        }
        return true;
    }

}
