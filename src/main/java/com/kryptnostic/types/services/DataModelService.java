package com.kryptnostic.types.services;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import com.kryptnostic.datastore.util.CassandraEdmMapping;
import com.kryptnostic.datastore.util.DatastoreConstants;
import com.kryptnostic.datastore.util.DatastoreConstants.Queries;
import com.kryptnostic.datastore.util.UUIDs;
import com.kryptnostic.datastore.util.UUIDs.ACLs;
import com.kryptnostic.types.Namespace;
import com.kryptnostic.types.ObjectType;
import com.kryptnostic.types.PropertyType;

public class DataModelService implements EdmManager {
    private static final Logger        logger = LoggerFactory.getLogger( DataModelService.class );

    private final Session              session;
    private final MappingManager       mappingManager;
    private final Mapper<Namespace>    namespaceMapper;
    private final Mapper<ObjectType>   objectTypeMapper;
    private final Mapper<PropertyType> propertyTypeMapper;

    private final CassandraEdmStore    edmStore;

    public DataModelService( Session session ) {
        createNamespaceTableIfNotExists( session );
        createObjectTypesTableIfNotExists( session );
        createPropertyTypesTableIfNotExists( session );

        this.session = session;
        this.mappingManager = new MappingManager( session );
        this.edmStore = mappingManager.createAccessor( CassandraEdmStore.class );
        this.namespaceMapper = mappingManager.mapper( Namespace.class );
        this.objectTypeMapper = mappingManager.mapper( ObjectType.class );
        this.propertyTypeMapper = mappingManager.mapper( PropertyType.class );

        upsertNamespace( new Namespace().setAclId( UUIDs.ACLs.EVERYONE_ACL )
                .setNamespace( DatastoreConstants.PRIMARY_NAMESPACE ) );

        createObjectType( new ObjectType().setNamespace( "kryptnostic" )
                .setKeys( ImmutableSet.of( "ssn", "passport" ) ).setType( "person" )
                .setTypename( Hashing.murmur3_128().hashString( "person", Charsets.UTF_8 ).toString() )
                .setAllowed( ImmutableSet.of() ) );
        upsertPropertyType( new PropertyType().setNamespace( "kryptnostic" ).setType( "SSN" )
                .setDatatype( EdmPrimitiveTypeKind.String ).setTypename( "ssn" ).setMultiplicity( 0 ) );
        Result<ObjectType> objectTypes = edmStore.getObjectTypes();
        Iterable<Namespace> namespaces = getNamespaces( ImmutableSet.of( ACLs.EVERYONE_ACL ) );
        namespaces.forEach( namespace -> logger.info( "Namespace loaded: {}", namespace ) );
        objectTypes.forEach( objectType -> logger.info( "Object read: {}", objectType ) );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#getNamespaces(java.util.List)
     */
    @Override
    public Iterable<Namespace> getNamespaces( Set<UUID> aclIds ) {
        return edmStore.getNamespaces( aclIds );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#updateObjectType(com.kryptnostic.types.ObjectType)
     */
    @Override
    public void upsertObjectType( ObjectType objectType ) {
        objectTypeMapper.save( objectType );
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.types.services.EdmManager#createObjectType(com.kryptnostic.types.ObjectType)
     */
    @Override
    public void createObjectType( ObjectType propertyType ) {
        edmStore.createObjectTypeIfNotExists( propertyType.getNamespace(),
                propertyType.getType(),
                propertyType.getTypename(),
                propertyType.getKeys(),
                propertyType.getAllowed() );
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
    public void createObjectType(
            String namespace,
            String type,
            String typename,
            Set<String> keys,
            Set<String> allowed ) {
        edmStore.createObjectTypeIfNotExists( namespace, type, typename, keys, allowed );
    }

    @Override
    public void createPropertyType(
            String namespace,
            String type,
            String typename,
            EdmPrimitiveTypeKind datatype,
            long multiplicity ) {
        edmStore.createPropertyTypeIfNotExists( namespace, type, typename, datatype, multiplicity );
    }

    @Override
    public void createNamespace( String namespace, UUID aclId ) {
        // Need to add API in edmStore
    }

    @Override
    public void deleteObjectType( ObjectType objectType ) {
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

    private static void createNamespaceTableIfNotExists( Session session ) {
        session.execute( DatastoreConstants.Queries.CREATE_NAMESPACE_TABLE );
    }

    private static void createObjectTypesTableIfNotExists( Session session ) {
        session.execute( DatastoreConstants.Queries.CREATE_OBJECT_TYPES_TABLE );

    }

    private void createPropertyTypesTableIfNotExists( Session session ) {
        session.execute( DatastoreConstants.Queries.CREATE_PROPERTY_TYPES_TABLE );
    }
}
