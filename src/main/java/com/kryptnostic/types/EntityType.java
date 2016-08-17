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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( key == null ) ? 0 : key.hashCode() );
        result = prime * result + ( ( namespace == null ) ? 0 : namespace.hashCode() );
        result = prime * result + ( ( properties == null ) ? 0 : properties.hashCode() );
        result = prime * result + ( ( type == null ) ? 0 : type.hashCode() );
        result = prime * result + ( ( typename == null ) ? 0 : typename.hashCode() );
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
        if ( !( obj instanceof EntityType ) ) {
            return false;
        }
        EntityType other = (EntityType) obj;
        if ( key == null ) {
            if ( other.key != null ) {
                return false;
            }
        } else if ( !key.equals( other.key ) ) {
            return false;
        }
        if ( namespace == null ) {
            if ( other.namespace != null ) {
                return false;
            }
        } else if ( !namespace.equals( other.namespace ) ) {
            return false;
        }
        if ( properties == null ) {
            if ( other.properties != null ) {
                return false;
            }
        } else if ( !properties.equals( other.properties ) ) {
            return false;
        }
        if ( type == null ) {
            if ( other.type != null ) {
                return false;
            }
        } else if ( !type.equals( other.type ) ) {
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
