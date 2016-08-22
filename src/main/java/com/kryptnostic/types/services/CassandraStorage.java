package com.kryptnostic.types.services;

import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.annotations.Query;
import com.kryptnostic.datastore.edm.Queries;

public interface CassandraStorage {
    @Query(Queries.INSERT_ENTITY_CLAUSES)
    ResultSet insertEntity( UUID aclId, UUID syncId, Set<String> entitySetNames );
}
