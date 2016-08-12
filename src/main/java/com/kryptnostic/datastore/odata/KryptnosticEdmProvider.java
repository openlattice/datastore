package com.kryptnostic.datastore.odata;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlAbstractEdmProvider;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainer;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityContainerInfo;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.ex.ODataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.odata.Ontology.EntitySchema;
import com.kryptnostic.datastore.util.UUIDs.ACLs;
import com.kryptnostic.types.EntitySet;
import com.kryptnostic.types.EntityType;
import com.kryptnostic.types.SchemaMetadata;
import com.kryptnostic.types.services.DataModelService;
import com.kryptnostic.types.services.EdmManager;

public class KryptnosticEdmProvider extends CsdlAbstractEdmProvider {
    private static final Logger                         logger           = LoggerFactory
            .getLogger( KryptnosticEdmProvider.class );
    // Service Namespace
    public static final String                          NAMESPACE        = "OData.Demo";

    // EDM Container
    public static final String                          CONTAINER_NAME   = "Container";
    public static final FullQualifiedName               CONTAINER        = new FullQualifiedName(
            NAMESPACE,
            CONTAINER_NAME );

    // Entity Types Names
    public static final String                          ET_PRODUCT_NAME  = "Product";
    public static final FullQualifiedName               ET_PRODUCT_FQN   = new FullQualifiedName(
            NAMESPACE,
            ET_PRODUCT_NAME );

    // Entity Set Names
    public static final String                          ES_PRODUCTS_NAME = "Products";

    private final EdmManager                            dms;

    private final IMap<String, FullQualifiedName>       entitySets;
    private final IMap<FullQualifiedName, EntitySchema> entitySchemas;

    public KryptnosticEdmProvider( HazelcastInstance hazelcast, EdmManager dms ) {
        this.dms = dms;
        this.entitySchemas = hazelcast.getMap( "entitySchemas" );
        this.entitySets = hazelcast.getMap( "entitySets" );

        dms.createPropertyType( NAMESPACE, "ID", "ID", EdmPrimitiveTypeKind.Int32, 0 );
        dms.createPropertyType( NAMESPACE, "Name", "Name", EdmPrimitiveTypeKind.String, 0 );
        dms.createPropertyType( NAMESPACE, "Description", "Description", EdmPrimitiveTypeKind.String, 0 );

        dms.createPropertyType( NAMESPACE, "aclId", "aclId", EdmPrimitiveTypeKind.Guid, 0 );
        dms.createPropertyType( NAMESPACE, "type", "type", EdmPrimitiveTypeKind.Guid, 0 );
        dms.createPropertyType( NAMESPACE, "clock", "clock", EdmPrimitiveTypeKind.Guid, 0 );
        dms.createPropertyType( NAMESPACE, "objectId", "version", EdmPrimitiveTypeKind.Int64, 0 );
        EntityType product = new EntityType().setNamespace( NAMESPACE ).setType( ET_PRODUCT_NAME )
                .setKey( ImmutableSet.of( "ID" ) ).setProperties( ImmutableSet.of( "ID", "Name", "Description" ) )
                .setTypename( ET_PRODUCT_NAME );
        EntityType metadataLevel = new EntityType().setNamespace( NAMESPACE ).setType( "metadataLevel" )
                .setKey( ImmutableSet.of( "aclId" ) )
                .setProperties( ImmutableSet.of( "aclId", "type", "clock", "objectId", "version" ) )
                .setTypename( "metadataLevel" );

        dms.createEntityType( product );
        dms.createEntityType( metadataLevel );

        dms.createSchema( NAMESPACE,
                "agora",
                ACLs.EVERYONE_ACL,
                ImmutableSet.of( product.getType(), metadataLevel.getType() ) );

        EntitySchema schema = new EntitySchema(
                ImmutableMap.<String, EdmPrimitiveTypeKind> builder()
                        .put( "ID", EdmPrimitiveTypeKind.Int32 )
                        .put( "Name", EdmPrimitiveTypeKind.String )
                        .put( "Description", EdmPrimitiveTypeKind.String )
                        .build(),
                ImmutableList.of( "ID" ) );
        EntitySchema schema2 = new EntitySchema(
                ImmutableMap.<String, EdmPrimitiveTypeKind> builder()
                        .put( "aclId", EdmPrimitiveTypeKind.Guid )
                        .put( "type", EdmPrimitiveTypeKind.Guid )
                        .put( "clock", EdmPrimitiveTypeKind.Guid )
                        .put( "objectId", EdmPrimitiveTypeKind.Guid )
                        .put( "version", EdmPrimitiveTypeKind.Int64 )
                        .build(),
                ImmutableList.of( "aclId" ) );

        entitySchemas.put( ET_PRODUCT_FQN, schema );
        entitySchemas.put( new FullQualifiedName( NAMESPACE, "metadataLevel" ), schema2 );
        entitySets.put( ES_PRODUCTS_NAME, ET_PRODUCT_FQN );
        entitySets.put( "metadataLevel", new FullQualifiedName(
                NAMESPACE,
                "metadataLevel" ) );
    }

    @Override
    public CsdlEntityType getEntityType( FullQualifiedName entityTypeName ) throws ODataException {
        // EntitySchema entityTypeDefs = entitySchemas.get( entityTypeName );

        EntityType objectType = dms.getEntityType( entityTypeName.getNamespace(), entityTypeName.getName() );

        return Transformers.transform( objectType );
        // CsdlEntityType entityTypeA = new CsdlEntityType();
        //
        // if ( entityTypeDefs != null ) {
        // entityTypeA.setName( entityTypeName.getName() );
        // entityTypeA.setKey( entityTypeDefs.getKeyProperties().stream()
        // .map( name -> new CsdlPropertyRef().setName( name ) ).collect( Collectors.toList() ) );
        // entityTypeA.setProperties(
        // entityTypeDefs.getProperties().entrySet().stream()
        // .map( ( prop ) -> new CsdlProperty().setName( prop.getKey() )
        // .setType( prop.getValue().getFullQualifiedName() ) )
        // .collect( Collectors.toList() ) );
        // return entityTypeA;
        // }

        // return null;
        // this method is called for one of the EntityTypes that are configured in the Schema
        // if ( entityTypeName.equals( ET_PRODUCT_FQN ) ) {
        //
        // // create EntityType properties
        // CsdlProperty id = new CsdlProperty().setName( "ID" )
        // .setType( EdmPrimitiveTypeKind.Int32.getFullQualifiedName() );
        // CsdlProperty name = new CsdlProperty().setName( "Name" )
        // .setType( EdmPrimitiveTypeKind.String.getFullQualifiedName() );
        // CsdlProperty description = new CsdlProperty().setName( "Description" )
        // .setType( EdmPrimitiveTypeKind.String.getFullQualifiedName() );
        //
        // // create CsdlPropertyRef for Key element
        // CsdlPropertyRef propertyRef = new CsdlPropertyRef();
        // propertyRef.setName( "ID" );
        //
        // // configure EntityType
        // CsdlEntityType entityType = new CsdlEntityType();
        // entityType.setName( ET_PRODUCT_NAME );
        // entityType.setProperties( Arrays.asList( id, name, description ) );
        // entityType.setKey( Collections.singletonList( propertyRef ) );
        //
        // return entityTypeA;
        // }

        // return null;
    }

    public CsdlEntitySet getEntitySet( FullQualifiedName entityContainer, String entitySetName ) {
        EntitySet entitySet = dms.getEntitySet( entityContainer.getNamespace(), entitySetName );
        return Transformers.transform( entitySet );
        // if ( entityContainer.equals( CONTAINER ) ) {
        // FullQualifiedName type = entitySets.get( entitySetName );
        // if ( type != null ) {
        // return new CsdlEntitySet().setName( entitySetName ).setType( type );
        // }
        // // if ( entitySetName.equals( ES_PRODUCTS_NAME ) ) {
        // // CsdlEntitySet entitySet = new CsdlEntitySet();
        // // entitySet.setName( ES_PRODUCTS_NAME );
        // // entitySet.setType( ET_PRODUCT_FQN );
        // //
        // // return entitySet;
        // // }
        // }

        // return null;
    }

    public CsdlEntityContainer getEntityContainer() {
        // create EntitySets
        List<CsdlEntitySet> entitySets = new ArrayList<CsdlEntitySet>();
        this.entitySets.keySet().forEach( e -> entitySets.add( getEntitySet( CONTAINER, e ) ) );
        // entitySets.add( getEntitySet( CONTAINER, ES_PRODUCTS_NAME ) );

        // create EntityContainer
        CsdlEntityContainer entityContainer = new CsdlEntityContainer();
        entityContainer.setName( CONTAINER_NAME );
        entityContainer.setEntitySets( entitySets );

        return entityContainer;
    }

    public List<CsdlSchema> getSchemas() throws ODataException {
        List<CsdlSchema> schemas = new ArrayList<CsdlSchema>();
        for ( SchemaMetadata schemaMetadata : dms.getSchemaMetadata() ) {
            CsdlSchema schema = new CsdlSchema();
            String namespace = schemaMetadata.getNamespace();
            schema.setNamespace( namespace );

            List<CsdlEntityType> entityTypes = schemaMetadata.getEntityTypes().parallelStream()
                    .map( type -> new FullQualifiedName( schemaMetadata.getNamespace(), type ) )
                    .map( fqn -> {
                        try {
                            return getEntityType( fqn );
                        } catch ( ODataException e ) {
                            logger.error( "Unable to get entity type for FQN={}", fqn );
                            return null;
                        }
                    } )
                    .filter( et -> et != null )
                    .collect( Collectors.toList() );
            schema.setEntityTypes( entityTypes );
            schema.setEntityContainer( getEntityContainer() );
            schemas.add( schema );
        }

        // create Schema
        // schema.setNamespace( NAMESPACE );
        //
        // // add EntityTypes
        // List<CsdlEntityType> entityTypes = new ArrayList<CsdlEntityType>();
        // this.entitySchemas.keySet().forEach( fqn -> {
        // try {
        // entityTypes.add( getEntityType( fqn ) );
        // } catch ( ODataException e ) {
        // logger.error( "Unstructured logging sucks!" );
        // }
        // } );
        // // entityTypes.add( getEntityType( ET_PRODUCT_FQN ) );
        // schema.setEntityTypes( entityTypes );
        //
        // // add EntityContainer
        // schema.setEntityContainer( getEntityContainer() );
        //
        // }
        // finally
        // schemas.add( schema );

        return schemas;
    }

    public CsdlEntityContainerInfo getEntityContainerInfo( FullQualifiedName entityContainerName ) {

        // This method is invoked when displaying the Service Document at e.g.
        // http://localhost:8080/DemoService/DemoService.svc
        if ( entityContainerName == null || entityContainerName.equals( CONTAINER ) ) {
            CsdlEntityContainerInfo entityContainerInfo = new CsdlEntityContainerInfo();
            entityContainerInfo.setContainerName( CONTAINER );
            return entityContainerInfo;
        }

        return null;
    }
}
