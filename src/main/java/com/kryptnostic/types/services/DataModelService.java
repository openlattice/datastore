package com.kryptnostic.types.services;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import com.kryptnostic.datastore.util.CassandraEdmMapping;
import com.kryptnostic.datastore.util.DatastoreConstants;
import com.kryptnostic.datastore.util.DatastoreConstants.Queries;
import com.kryptnostic.datastore.util.UUIDs;
import com.kryptnostic.datastore.util.UUIDs.ACLs;
import com.kryptnostic.types.Container;
import com.kryptnostic.types.EntitySet;
import com.kryptnostic.types.Namespace;
import com.kryptnostic.types.EntityType;
import com.kryptnostic.types.PropertyType;
import com.kryptnostic.types.Schema;

public class DataModelService implements EdmManager {
    private static final Logger        logger = LoggerFactory.getLogger( DataModelService.class );

    private final Session              session;
    private final MappingManager       mappingManager;
    private final Mapper<Namespace>    namespaceMapper;
    private final Mapper<Container>    containerMapper;
    private final Mapper<EntitySet>    entitySetMapper;
    private final Mapper<EntityType>   objectTypeMapper;
    private final Mapper<PropertyType> propertyTypeMapper;

    private final CassandraEdmStore    edmStore;

    public DataModelService( Session session ) {
        createNamespaceTableIfNotExists( session );
        createEntityTypesTableIfNotExists( session );
        createPropertyTypesTableIfNotExists( session );
        createEntitySetTableIfNotExists( session );

        this.session = session;
        this.mappingManager = new MappingManager( session );
        this.edmStore = mappingManager.createAccessor( CassandraEdmStore.class );
        this.namespaceMapper = mappingManager.mapper( Namespace.class );
        this.containerMapper = mappingManager.mapper( Container.class );
        this.entitySetMapper = mappingManager.mapper( EntitySet.class );
        this.objectTypeMapper = mappingManager.mapper( EntityType.class );
        this.propertyTypeMapper = mappingManager.mapper( PropertyType.class );

        upsertNamespace( new Namespace().setAclId( UUIDs.ACLs.EVERYONE_ACL )
                .setNamespace( DatastoreConstants.PRIMARY_NAMESPACE ) );
        createObjectType( new EntityType().setNamespace( "kryptnostic" )
                .setKeys( ImmutableSet.of( "ssn", "passport" ) ).setType( "person" )
                .setTypename( Hashing.murmur3_128().hashString( "person", Charsets.UTF_8 ).toString() )
                .setAllowed( ImmutableSet.of() ) );
        upsertPropertyType( new PropertyType().setNamespace( "kryptnostic" ).setType( "SSN" )
                .setDatatype( EdmPrimitiveTypeKind.String ).setTypename( "ssn" ).setMultiplicity( 0 ) );
        Result<EntityType> objectTypes = edmStore.getObjectTypes();
        Iterable<Namespace> namespaces = getNamespaces();
        namespaces.forEach( namespace -> logger.info( "Namespace loaded: {}", namespace ) );
        objectTypes.forEach( objectType -> logger.info( "Object read: {}", objectType ) );
    }

    @Override
    public Schema getSchema( String namespace ) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#getNamespaces(java.util.List)
     */
    @Override
    public Iterable<Namespace> getNamespaces() {
        // TODO: Actually grab ACLs based on the current user.
        return edmStore.getNamespaces( ImmutableList.of( ACLs.EVERYONE_ACL ) );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#updateObjectType(com.kryptnostic.types.ObjectType)
     */
    @Override
    public void upsertObjectType( EntityType objectType ) {
        objectTypeMapper.save( objectType );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#createObjectType(com.kryptnostic.types.ObjectType)
     */
    @Override
    public boolean createObjectType( EntityType propertyType ) {
        return wasLightweightTransactionApplied( edmStore.createObjectTypeIfNotExists( propertyType.getNamespace(),
                propertyType.getType(),
                propertyType.getTypename(),
                propertyType.getKey(),
                propertyType.getAllowed() ) );
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
    public void upsertNamespace( Namespace namespace ) {
        namespaceMapper.save( namespace );
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
    public void createNamespace( String namespace, UUID aclId ) {
        edmStore.createNamespaceIfNotExists( namespace, aclId );
    }

    @Override
    public void deleteObjectType( EntityType objectType ) {
        objectTypeMapper.delete( objectType );
    }

    @Override
    public void deletePropertyType( PropertyType propertyType ) {
        propertyTypeMapper.delete( propertyType );
    }

    @Override
    public void deleteNamespace( Namespace namespaces ) {
        /*
         * TODO: Implement delete namespace 1. Implement AccessCheck 2. Remove all property types and object types
         * associated with namespace. 3. For each removed property type and each removed object type remove
         * corresponding object and property tables 4. Delete the schema.
         */

        // namespaceMapper.delete( namespaces );

    }

    @Override
    public boolean createContainer( String namespace, Container container ) {
        return wasLightweightTransactionApplied( edmStore.createContainerIfNotExists( container.getNamespace(),
                container.getContainer(),
                container.getObjectTypes() ) );
    }

    @Override
    public void upsertContainer( Container container ) {
        containerMapper.save( container );
    }

    @Override
    public void addEntityTypesToContainer( String namespace, String container, Set<String> entityTypes ) {
        edmStore.addEntityTypesToContainer( namespace, container, entityTypes );
    }

    @Override
    public void removeEntityTypesFromContainer( String namespace, String container, Set<String> entityTypes ) {
        edmStore.removeEntityTypesFromContainer( namespace, container, entityTypes );

    }

    @Override
    public boolean createEntitySet( String namespace, EntitySet entitySet ) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void upsertEntitySet( EntitySet entitySet ) {
        // TODO Auto-generated method stub

    }

    private static void createNamespaceTableIfNotExists( Session session ) {
        session.execute( Queries.CREATE_NAMESPACE_TABLE );
    }

    private static void createEntitySetTableIfNotExists( Session session ) {
        session.execute( Queries.CREATE_ENTITY_SET_TABLE );
    }

    private static void createEntityTypesTableIfNotExists( Session session ) {
        session.execute( Queries.CREATE_ENTITY_TYPES_TABLE );
        session.execute( Queries.CREATE_INDEX_ON_TYPENAME );
    }

    private void createPropertyTypesTableIfNotExists( Session session ) {
        session.execute( DatastoreConstants.Queries.CREATE_PROPERTY_TYPES_TABLE );
    }

    private static boolean wasLightweightTransactionApplied( ResultSet rs ) {
        return rs.one().getBool( DatastoreConstants.APPLIED_FIELD );
    }

    public EntityType getEntityType( String namespace, String name ) {
        return objectTypeMapper.get( namespace, name );
    }

    public EntitySet getEntitySet( String namespace, String name, String entitySetName ) {
        return entitySetMapper.get( namespace, name, entitySetName );
    }

}
