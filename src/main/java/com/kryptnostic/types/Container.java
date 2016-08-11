package com.kryptnostic.types;

import java.util.UUID;

import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainer;

import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.kryptnostic.datastore.util.DatastoreConstants;

/**
 * This class roughly correspond to {@link CsdlEntityContainer} and is annotated for use by the {@link MappingManager}
 * to R/W from Cassandra.
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 *
 */
@Table(
    keyspace = DatastoreConstants.KEYSPACE,
    name = DatastoreConstants.CONTAINERS_TABLE )
public class Container extends Namespace {
    @PartitionKey(
        value = 1 )
    private String container;

    @Override
    public Container setAclId( UUID aclId ) {
        return (Container) super.setAclId( aclId );
    }

    @Override
    public Container setNamespace( String namespace ) {
        return (Container) super.setNamespace( namespace );
    }

    public String getContainer() {
        return container;
    }

    public Container setContainer( String container ) {
        this.container = container;
        return this;
    }

}
