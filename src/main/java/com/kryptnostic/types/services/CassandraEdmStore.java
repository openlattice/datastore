package com.kryptnostic.types.services;

import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import com.datastax.driver.mapping.annotations.QueryParameters;
import com.kryptnostic.types.ObjectType;

@Accessor
public interface CassandraEdmStore {
    @Query( "select * from sparks.object_types" )
    @QueryParameters
    public Result<ObjectType> getObjectTypes();
    

}
