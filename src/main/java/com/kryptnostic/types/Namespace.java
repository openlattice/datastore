package com.kryptnostic.types;

import java.util.UUID;

import org.apache.olingo.commons.api.edm.provider.CsdlSchema;

import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.kryptnostic.datastore.util.DatastoreConstants;

/**
 * This class roughly corresponds to {@link CsdlSchema} and is annotated for use by the {@link MappingManager} to R/W
 * from Cassandra.
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 * 
 */
@Table(
    keyspace = DatastoreConstants.KEYSPACE,
    name = DatastoreConstants.NAMESPACE_TABLE )
public class Namespace {
    @PartitionKey(value=0)
    private String namespace;
    @ClusteringColumn(value=0)
    private UUID   aclId;

    public String getNamespace() {
        return namespace;
    }

    public Namespace setNamespace( String namespace ) {
        this.namespace = namespace;
        return this;
    }

    public UUID getAclId() {
        return aclId;
    }

    public Namespace setAclId( UUID aclId ) {
        this.aclId = aclId;
        return this;
    }

    @Override
    public String toString() {
        return "Namespace [namespace=" + namespace + ", aclId=" + aclId + "]";
    }

}
