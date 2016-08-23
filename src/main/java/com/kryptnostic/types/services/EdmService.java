package com.kryptnostic.types.services;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Futures;
import com.kryptnostic.conductor.rpc.UUIDs;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.conductor.rpc.odata.Schema;
import com.kryptnostic.datastore.odata.EntityDataModel;
import com.kryptnostic.datastore.util.Util;

public class EdmService implements EdmManager {
    private static final Logger         logger = LoggerFactory.getLogger( EdmService.class );

    private final Session               session;
    private final Mapper<Schema>        schemaMapper;
    private final Mapper<EntitySet>     entitySetMapper;
    private final Mapper<EntityType>    entityTypeMapper;
    private final Mapper<PropertyType>  propertyTypeMapper;

    private final CassandraEdmStore     edmStore;
    private final CassandraTableManager tableManager;

    public EdmService( Session session, MappingManager mappingManager, CassandraTableManager tableManager ) {
        this.session = session;
        this.edmStore = mappingManager.createAccessor( CassandraEdmStore.class );
        this.schemaMapper = mappingManager.mapper( Schema.class );
        this.entitySetMapper = mappingManager.mapper( EntitySet.class );
        this.entityTypeMapper = mappingManager.mapper( EntityType.class );
        this.propertyTypeMapper = mappingManager.mapper( PropertyType.class );
        this.tableManager = tableManager;
        upsertSchema( new Schema().setAclId( UUIDs.ACLs.EVERYONE_ACL )
                .setNamespace( DatastoreConstants.PRIMARY_NAMESPACE ).setName( DatastoreConstants.PRIMARY_NAMESPACE ) );
        createEntityType( new EntityType().setNamespace( DatastoreConstants.PRIMARY_NAMESPACE )
                .setKey( ImmutableSet.of( new FullQualifiedName( DatastoreConstants.PRIMARY_NAMESPACE, "ssn" ),
                        new FullQualifiedName( DatastoreConstants.PRIMARY_NAMESPACE, "passport" ) ) )
                .setType( "person" )
                .setTypename( Hashing.murmur3_128().hashString( "person", Charsets.UTF_8 ).toString() )
                .setProperties( ImmutableSet.of() ) );
        upsertPropertyType( new PropertyType().setNamespace( "kryptnostic" ).setName( "SSN" )
                .setDatatype( EdmPrimitiveTypeKind.String ).setTypename( "ssn" ).setMultiplicity( 0 ) );
        Result<EntityType> objectTypes = edmStore.getEntityTypes();
        Iterable<Schema> namespaces = getSchemas();
        namespaces.forEach( namespace -> logger.info( "Namespace loaded: {}", namespace ) );
        objectTypes.forEach( objectType -> logger.info( "Object read: {}", objectType ) );
    }

    @Override
    public Schema getSchema( String namespace, String name ) {
        return schemaMapper.get( namespace, ACLs.EVERYONE_ACL, name );
    }

    @Override
    public void enrichSchemaWithEntityTypes( Schema schema ) {
        Set<EntityType> entityTypes = schema.getEntityTypeFqns().stream()
                .map( type -> entityTypeMapper.getAsync( schema.getNamespace(), type ) )
                .map( futureEntityType -> Util.getFutureSafely( futureEntityType ) ).filter( e -> e != null )
                .collect( Collectors.toSet() );
        schema.addEntityTypes( entityTypes );
    }

    @Override
    public void enrichSchemaWithPropertyTypes( Schema schema ) {
        Set<FullQualifiedName> propertyTypeNames = Sets.newHashSet();

        if ( schema.getEntityTypes().isEmpty() && !schema.getEntityTypeFqns().isEmpty() ) {
            enrichSchemaWithEntityTypes( schema );
        }

        for ( EntityType entityType : schema.getEntityTypes() ) {
            propertyTypeNames.addAll( entityType.getProperties() );
        }

        Set<PropertyType> propertyTypes = propertyTypeNames.stream()
                .map( type -> propertyTypeMapper.getAsync( type.getNamespace(), type.getName() ) )
                .map( futurePropertyType -> Util.getFutureSafely( futurePropertyType ) ).filter( e -> e != null )
                .collect( Collectors.toSet() );

        schema.addPropertyTypes( propertyTypes );

    }

    @Override
    public Iterable<Schema> getSchemasInNamespace( String namespace ) {
        return edmStore.getSchemas( namespace, ImmutableList.of( ACLs.EVERYONE_ACL ) );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#getNamespaces(java.util.List)
     */
    @Override
    public Iterable<Schema> getSchemas() {
        // TODO: Actually grab ACLs based on the current user.
        return edmStore.getSchemas( ImmutableList.of( ACLs.EVERYONE_ACL ) );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#updateObjectType(com.kryptnostic.types.ObjectType)
     */
    @Override
    public void upsertEntityType( EntityType objectType ) {
        entityTypeMapper.save( objectType );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#createPropertyType(com.kryptnostic.types.PropertyType)
     */
    @Override
    public void upsertPropertyType( PropertyType propertyType ) {
        // Create or retrieve it's typename.
        String typename = tableManager.getTypenameForPropertyType( propertyType );
        propertyType.setTypename( typename );
        propertyTypeMapper.save( propertyType );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#createNamespace(com.kryptnostic.types.Namespace)
     */
    @Override
    public void upsertSchema( Schema namespace ) {
        schemaMapper.save( namespace );
    }

    @Override
    public boolean createPropertyType( PropertyType propertyType ) {
        /*
         * We retrieve or create the typename for the property. If the property already exists then lightweight
         * transaction will fail and return value will be correctly set.
         */
        String typename = tableManager.getTypenameForPropertyType( propertyType );
        return Util.wasLightweightTransactionApplied(
                edmStore.createPropertyTypeIfNotExists( propertyType.getNamespace(),
                        propertyType.getName(),
                        typename,
                        propertyType.getDatatype(),
                        propertyType.getMultiplicity() ) );
    }

    @Override
    public boolean createSchema( String namespace, String name, UUID aclId, Set<FullQualifiedName> entityTypes ) {
        return Util.wasLightweightTransactionApplied(
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
    public void deleteSchema( Schema namespaces ) {
        // TODO: 1. Implement AccessCheck

        schemaMapper.delete( namespaces );
    }

    @Override
    public void addEntityTypesToSchema( String namespace, String name, Set<FullQualifiedName> entityTypes ) {
        edmStore.addEntityTypesToContainer( namespace, ACLs.EVERYONE_ACL, name, entityTypes );
    }

    @Override
    public void removeEntityTypesFromSchema( String namespace, String name, Set<FullQualifiedName> entityTypes ) {
        edmStore.removeEntityTypesFromContainer( namespace, ACLs.EVERYONE_ACL, name, entityTypes );
    }

    @Override
    public boolean createEntityType(
            EntityType entityType ) {
        boolean entityCreated = false;
        if ( propertiesExist( entityType.getProperties() )
                && entityType.getProperties().containsAll( entityType.getKey() ) ) {

            String typename = tableManager.createEntityTypeTable( entityType );
            entityCreated = Util.wasLightweightTransactionApplied(
                    edmStore.createEntityTypeIfNotExists( entityType.getNamespace(),
                            entityType.getName(),
                            typename,
                            entityType.getKey(),
                            entityType.getProperties() ) );
        }
        return entityCreated;
    }

    private boolean propertiesExist( Set<FullQualifiedName> properties ) {
        // TODO: Extract contents
        Stream<ResultSetFuture> futures = properties.parallelStream()
                .map( prop -> session
                        .executeAsync(
                                QueryBuilder.select().countAll().from( DatastoreConstants.PROPERTY_TYPES_TABLE )
                                        .where( QueryBuilder.eq( "namespace", prop.getNamespace() ) )
                                        .and( QueryBuilder.eq( "name", prop.getName() ) ) ) );
        // Cause Java 8
        try {
            return Futures.allAsList( (Iterable<ResultSetFuture>) futures::iterator ).get().stream()
                    .map( ResultSet::one )
                    .map( row -> row.getInt( "count" ) ).filter( count -> count == 0 ).collect( Collectors.counting() )
                    .intValue() == 0;
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Unable to verify all properties exist." );
            return false;
        }
    }

    @Override
    public void upsertEntitySet( EntitySet entitySet ) {
        entitySetMapper.save( entitySet );
    }

    @Override
    public void deleteEntitySet( EntitySet entitySet ) {
        entitySetMapper.delete( entitySet );
    }

    @Override
    public boolean createEntitySet( FullQualifiedName type, String name, String title ) {
        return Util.wasLightweightTransactionApplied( edmStore.createEntitySet( type, name, title ) );
    }

    @Override
    public boolean createEntitySet( EntitySet entitySet ) {
        return createEntitySet( entitySet.getType(), entitySet.getName(), entitySet.getTitle() );
    }

    public EntityType getEntityType( String namespace, String name ) {
        return entityTypeMapper.get( namespace, name );
    }

    public EntitySet getEntitySet( FullQualifiedName type, String name ) {
        return entitySetMapper.get( type, name );
    }

    public EntitySet getEntitySet( String name ) {
        return edmStore.getEntitySet( name );
    }

    @Override
    public Iterable<EntitySet> getEntitySets() {
        return edmStore.getEntitySets();
    }

    @Override
    public PropertyType getPropertyType( FullQualifiedName propertyType ) {
        return propertyTypeMapper.get( propertyType.getNamespace(), propertyType.getName() );
    }

    @Override
    public EntityDataModel getEntityDataModel() {
        Iterable<Schema> schemas = getSchemas();
        final Set<EntityType> entityTypes = Sets.newHashSet();
        final Set<PropertyType> propertyTypes = Sets.newHashSet();
        final Set<EntitySet> entitySets = Sets.newHashSet();
        final Set<String> namespaces = Sets.newHashSet();

        schemas.forEach( schema -> {
            enrichSchemaWithEntityTypes( schema );
            enrichSchemaWithPropertyTypes( schema );
            entityTypes.addAll( schema.getEntityTypes() );
            propertyTypes.addAll( schema.getPropertyTypes() );
            schema.getEntityTypes().forEach( entityType -> namespaces.add( entityType.getNamespace() ) );
            schema.getPropertyTypes().forEach( propertyType -> namespaces.add( propertyType.getNamespace() ) );

        } );

        return new EntityDataModel(
                namespaces,
                ImmutableSet.copyOf( schemas ),
                entityTypes,
                propertyTypes,
                entitySets );
    }

    @Override
    public boolean isExistingEntitySet( FullQualifiedName type, String name ) {
        return Util.isCountNonZero( edmStore.countEntitySet( type, name ) );
    }

}
