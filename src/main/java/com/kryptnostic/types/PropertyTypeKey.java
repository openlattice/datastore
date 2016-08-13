package com.kryptnostic.types;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;

public class PropertyTypeKey {
    @PartitionKey(
        value = 0 )
    protected String             namespace;
    @PartitionKey(
        value = 1 )
    protected String             type;

    @Column(
        name = "datatype" )
    protected EdmPrimitiveTypeKind datatype;

    public String getNamespace() {
        return namespace;
    }

    public PropertyTypeKey setNamespace( String namespace ) {
        this.namespace = namespace;
        return this;
    }

    public String getType() {
        return type;
    }

    public PropertyTypeKey setType( String type ) {
        this.type = type;
        return this;
    }

    public EdmPrimitiveTypeKind getDatatype() {
        return datatype;
    }

    public PropertyTypeKey setDatatype( EdmPrimitiveTypeKind datatype ) {
        this.datatype = datatype;
        return this;
    }

    @Override
    public String toString() {
        return "PropertyTypeKey [namespace=" + namespace + ", type=" + type + ", datatype=" + datatype + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( datatype == null ) ? 0 : datatype.hashCode() );
        result = prime * result + ( ( namespace == null ) ? 0 : namespace.hashCode() );
        result = prime * result + ( ( type == null ) ? 0 : type.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( obj == null ) {
            return false;
        }
        if ( !( obj instanceof PropertyTypeKey ) ) {
            return false;
        }
        PropertyTypeKey other = (PropertyTypeKey) obj;
        if ( datatype != other.datatype ) {
            return false;
        }
        if ( namespace == null ) {
            if ( other.namespace != null ) {
                return false;
            }
        } else if ( !namespace.equals( other.namespace ) ) {
            return false;
        }
        if ( type == null ) {
            if ( other.type != null ) {
                return false;
            }
        } else if ( !type.equals( other.type ) ) {
            return false;
        }
        return true;
    }

}
