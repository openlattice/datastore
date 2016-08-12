package com.kryptnostic.types.services;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.kryptnostic.datastore.util.CassandraEdmMapping;
import com.kryptnostic.datastore.util.DatastoreConstants;
import com.kryptnostic.datastore.util.DatastoreConstants.Queries;
import com.kryptnostic.datastore.util.UUIDs;
import com.kryptnostic.datastore.util.UUIDs.ACLs;
import com.kryptnostic.datastore.util.Util;
import com.kryptnostic.types.EntitySet;
import com.kryptnostic.types.EntityType;
import com.kryptnostic.types.PropertyType;
import com.kryptnostic.types.Schema;
import com.kryptnostic.types.SchemaMetadata;

public class DataModelService implements EdmManager {
    private static final Logger          logger = LoggerFactory.getLogger( DataModelService.class );

    private final Session                session;
    private final MappingManager         mappingManager;
    private final Mapper<SchemaMetadata> schemaMapper;
    private final Mapper<EntitySet>      entitySetMapper;
    private final Mapper<EntityType>     entityTypeMapper;
    private final Mapper<PropertyType>   propertyTypeMapper;

    private final CassandraEdmStore      edmStore;

    public DataModelService( Session session ) {
        createSchemasTableIfNotExists( session );
        createEntityTypesTableIfNotExists( session );
        createPropertyTypesTableIfNotExists( session );
        createEntitySetTableIfNotExists( session );

        this.session = session;
        this.mappingManager = new MappingManager( session );
        this.edmStore = mappingManager.createAccessor( CassandraEdmStore.class );
        this.schemaMapper = mappingManager.mapper( SchemaMetadata.class );
        this.entitySetMapper = mappingManager.mapper( EntitySet.class );
        this.entityTypeMapper = mappingManager.mapper( EntityType.class );
        this.propertyTypeMapper = mappingManager.mapper( PropertyType.class );

        upsertSchema( new SchemaMetadata().setAclId( UUIDs.ACLs.EVERYONE_ACL )
                .setNamespace( DatastoreConstants.PRIMARY_NAMESPACE ).setName( DatastoreConstants.PRIMARY_NAMESPACE ) );
        createEntityType( new EntityType().setNamespace( "kryptnostic" )
                .setKey( ImmutableSet.of( "ssn", "passport" ) ).setType( "person" )
                .setTypename( Hashing.murmur3_128().hashString( "person", Charsets.UTF_8 ).toString() )
                .setProperties( ImmutableSet.of() ) );
        upsertPropertyType( new PropertyType().setNamespace( "kryptnostic" ).setType( "SSN" )
                .setDatatype( EdmPrimitiveTypeKind.String ).setTypename( "ssn" ).setMultiplicity( 0 ) );
        Result<EntityType> objectTypes = edmStore.getObjectTypes();
        Iterable<SchemaMetadata> namespaces = getSchemaMetadata();
        namespaces.forEach( namespace -> logger.info( "Namespace loaded: {}", namespace ) );
        objectTypes.forEach( objectType -> logger.info( "Object read: {}", objectType ) );
    }

    @Override
    public Schema getSchema( String namespace, String name ) {
        SchemaMetadata schemaMetadata = schemaMapper.get( namespace, ACLs.EVERYONE_ACL, name );
        // entityTypeMapper.get
        Set<EntityType> entityTypes = schemaMetadata.getEntityTypes().stream()
                .map( type -> entityTypeMapper.getAsync( schemaMetadata.getNamespace(), type ) )
                .map( futureEntityType -> Util.getFutureSafely( futureEntityType ) ).filter( e -> e != null )
                .collect( Collectors.toSet() );
        Set<String> propertyTypeNames = Sets.newHashSet();

        for ( EntityType entityType : entityTypes ) {
            propertyTypeNames.addAll( entityType.getProperties() );
        }

        Set<PropertyType> propertyTypes = propertyTypeNames.stream()
                .map( type -> propertyTypeMapper.getAsync( schemaMetadata.getNamespace(), type ) )
                .map( futurePropertyType -> Util.getFutureSafely( futurePropertyType ) ).filter( e -> e != null )
                .collect( Collectors.toSet() );

        return new Schema( entityTypes, propertyTypes, Optional.of( schemaMetadata.getAclId() ) );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#getNamespaces(java.util.List)
     */
    @Override
    public Iterable<SchemaMetadata> getSchemaMetadata() {
        // TODO: Actually grab ACLs based on the current user.
        return edmStore.getSchemaMetadata( ImmutableList.of( ACLs.EVERYONE_ACL ) );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#updateObjectType(com.kryptnostic.types.ObjectType)
     */
    @Override
    public void upsertObjectType( EntityType objectType ) {
        entityTypeMapper.save( objectType );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#createObjectType(com.kryptnostic.types.ObjectType)
     */
    @Override
    public boolean createEntityType( EntityType entityType ) {
        return createEntityType( entityType.getNamespace(),
                entityType.getType(),
                entityType.getTypename(),
                entityType.getKey(),
                entityType.getProperties() );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#createPropertyType(com.kryptnostic.types.PropertyType)
     */
    @Override
    public void upsertPropertyType( PropertyType propertyType ) {
        // Create the property
        edmStore.createPropertyTypeIfNotExists( propertyType.getNamespace(),
                propertyType.getType(),
                propertyType.getTypename(),
                propertyType.getDatatype(),
                propertyType.getMultiplicity() );
        // Create the property specific table
        String propertTableQuery = String.format( Queries.PROPERTY_TABLE,
                propertyType.getTypename(),
                CassandraEdmMapping.getCassandraTypeName( propertyType.getDatatype() ) );
        session.execute( propertTableQuery );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#createNamespace(com.kryptnostic.types.Namespace)
     */
    @Override
    public void upsertSchema( SchemaMetadata namespace ) {
        schemaMapper.save( namespace );
    }

    @Override
    public boolean createPropertyType(
            String namespace,
            String type,
            String typename,
            EdmPrimitiveTypeKind datatype,
            long multiplicity ) {
        // TODO: Verify that this returning the proper value.
        return wasLightweightTransactionApplied(
                edmStore.createPropertyTypeIfNotExists( namespace, type, typename, datatype, multiplicity ) );
    }

    @Override
    public boolean createSchema( String namespace, String name, UUID aclId, Set<String> entityTypes ) {
        return wasLightweightTransactionApplied(
                edmStore.createSchemaIfNotExists( namespace, name, aclId, entityTypes ) );
    }

    @Override
    public void deleteEntityType( EntityType objectType ) {
        entityTypeMapper.delete( objectType );
    }

    @Override
    public void deletePropertyType( PropertyType propertyType ) {
        propertyTypeMapper.delete( propertyType );
    }

    @Override
    public void deleteSchema( SchemaMetadata namespaces ) {
        // TODO: 1. Implement AccessCheck

        schemaMapper.delete( namespaces );
    }

    @Override
    public void addEntityTypesToSchema( String namespace, String name, Set<String> entityTypes ) {
         edmStore.addEntityTypesToContainer( namespace, ACLs.EVERYONE_ACL, name, entityTypes );
    }

    @Override
    public void removeEntityTypesFromSchema( String namespace, String name, Set<String> entityTypes ) {
         edmStore.removeEntityTypesFromContainer( namespace, ACLs.EVERYONE_ACL, name, entityTypes );
    }

    @Override
    public boolean createEntityType(
            String namespace,
            String type,
            String typename,
            Set<String> key,
            Set<String> properties ) {
        return wasLightweightTransactionApplied(
                edmStore.createEntityTypeIfNotExists( namespace, type, typename, key, properties ) );
    }

    @Override
    public void upsertEntitySet( EntitySet entitySet ) {
        entitySetMapper.save( entitySet );
    }

    private static void createSchemasTableIfNotExists( Session session ) {
        session.execute( Queries.CREATE_SCHEMAS_TABLE );
    }

    private static void createEntitySetTableIfNotExists( Session session ) {
        session.execute( Queries.CREATE_ENTITY_SETS_TABLE );
        session.execute( Queries.CREATE_INDEX_ON_TYPE );
    }

    private static void createEntityTypesTableIfNotExists( Session session ) {
        session.execute( Queries.CREATE_ENTITY_TYPES_TABLE );
    }

    private void createPropertyTypesTableIfNotExists( Session session ) {
        session.execute( DatastoreConstants.Queries.CREATE_PROPERTY_TYPES_TABLE );
    }

    private static boolean wasLightweightTransactionApplied( ResultSet rs ) {
        return rs.one().getBool( DatastoreConstants.APPLIED_FIELD );
    }

    public EntityType getEntityType( String namespace, String name ) {
        return entityTypeMapper.get( namespace, name );
    }

    public EntitySet getEntitySet( String namespace, String name ) {
        return entitySetMapper.get( namespace, name );
    }

    @Override
    public void deleteEntitySet( EntitySet entitySet ) {
        // TODO Auto-generated method stub
        
    }

}
