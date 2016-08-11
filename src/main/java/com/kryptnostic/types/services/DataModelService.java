package com.kryptnostic.types.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.types.ObjectType;

public class DataModelService {
    private static final Logger     logger = LoggerFactory.getLogger( DataModelService.class );

    private final MappingManager    mappingManager;
    private final CassandraEdmStore edmStore;

    public DataModelService( Session session ) {
        this.mappingManager = new MappingManager( session );
        this.edmStore = mappingManager.createAccessor( CassandraEdmStore.class );
        Mapper<ObjectType> objectTypeMapper = mappingManager.mapper( ObjectType.class );
        objectTypeMapper.save( new ObjectType().setNamespace( "kryptnostic" )
                .setKeys( ImmutableSet.of( "ssn", "passport" ) ).setType( "person" ) );
        Result<ObjectType> objectTypes = edmStore.getObjectTypes( );
        objectTypes.forEach( objectType -> logger.error( "Object read: {}", objectType ) );
    }
}
