package com.kryptnostic.types;

import java.util.UUID;

import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.kryptnostic.datastore.util.DatastoreConstants;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt; Class for use by the {@link MappingManager} to R/W
 *         namespaces from Cassandra.
 */
@Table(
    keyspace = DatastoreConstants.KEYSPACE,
    name = DatastoreConstants.NAMESPACE_TABLE )
public class Namespace {
    @PartitionKey
    private String namespace;
    @ClusteringColumn
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
