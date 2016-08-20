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

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.odata.Ontology.EntitySchema;
import com.kryptnostic.datastore.odata.Transformers.EntityTypeTransformer;
import com.kryptnostic.datastore.util.UUIDs.ACLs;
import com.kryptnostic.types.EntitySet;
import com.kryptnostic.types.EntityType;
import com.kryptnostic.types.Schema;
import com.kryptnostic.types.services.EdmManager;

import jersey.repackaged.com.google.common.collect.Lists;

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
    private final EntityTypeTransformer                 ett;
    private final IMap<String, FullQualifiedName>       entitySets;
    private final IMap<FullQualifiedName, EntitySchema> entitySchemas;

    public KryptnosticEdmProvider( HazelcastInstance hazelcast, EdmManager dms ) {
        this.dms = dms;
        this.ett = new EntityTypeTransformer( dms );
        this.entitySchemas = hazelcast.getMap( "entitySchemas" );
        this.entitySets = hazelcast.getMap( "entitySets" );

        dms.createPropertyType( NAMESPACE, "ID", "ID", EdmPrimitiveTypeKind.Int32, 0 );
        dms.createPropertyType( NAMESPACE, "Name", "Name", EdmPrimitiveTypeKind.String, 0 );
        dms.createPropertyType( NAMESPACE, "Description", "Description", EdmPrimitiveTypeKind.String, 0 );

        dms.createPropertyType( NAMESPACE, "aclId", "aclId", EdmPrimitiveTypeKind.Guid, 0 );
        dms.createPropertyType( NAMESPACE, "type", "type", EdmPrimitiveTypeKind.Guid, 0 );
        dms.createPropertyType( NAMESPACE, "clock", "clock", EdmPrimitiveTypeKind.Guid, 0 );
        dms.createPropertyType( NAMESPACE, "objectId", "objectId", EdmPrimitiveTypeKind.Int64, 0 );
        dms.createPropertyType( NAMESPACE, "version", "version", EdmPrimitiveTypeKind.Int64, 0 );
        EntityType product = new EntityType().setNamespace( NAMESPACE ).setType( ET_PRODUCT_NAME )
                .setKey( ImmutableSet.of( new FullQualifiedName( NAMESPACE, "ID" ) ) )
                .setProperties( ImmutableSet.of( new FullQualifiedName( NAMESPACE, "ID" ),
                        new FullQualifiedName( NAMESPACE, "Name" ),
                        new FullQualifiedName( NAMESPACE, "Description" ) ) )
                .setTypename( ET_PRODUCT_NAME );
        EntityType metadataLevel = new EntityType().setNamespace( NAMESPACE ).setType( "metadataLevel" )
                .setKey( ImmutableSet.of( new FullQualifiedName( NAMESPACE, "aclId" ) ) )
                .setProperties( ImmutableSet.of( new FullQualifiedName( NAMESPACE, "aclId" ),
                        new FullQualifiedName( NAMESPACE, "type" ),
                        new FullQualifiedName( NAMESPACE, "clock" ),
                        new FullQualifiedName( NAMESPACE, "objectId" ),
                        new FullQualifiedName( NAMESPACE, "version" ) ) )
                .setTypename( "metadataLevel" );

        dms.createEntityType( product );
        dms.createEntityType( metadataLevel );
        dms.createEntitySet( ET_PRODUCT_FQN, ES_PRODUCTS_NAME, null );
        dms.createEntitySet( new FullQualifiedName( NAMESPACE, "metadataLevel" ), "metadataLevels", null );

        dms.createSchema( NAMESPACE,
                "agora",
                ACLs.EVERYONE_ACL,
                ImmutableSet.of( new FullQualifiedName( NAMESPACE, product.getType() ),
                        new FullQualifiedName( NAMESPACE, metadataLevel.getType() ) ) );

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
        entitySets.put( "metadataLevels", new FullQualifiedName(
                NAMESPACE,
                "metadataLevel" ) );
    }

    @Override
    public CsdlEntityType getEntityType( FullQualifiedName entityTypeName ) throws ODataException {
        EntityType objectType = dms.getEntityType( entityTypeName.getNamespace(), entityTypeName.getName() );

        return ett.transform( objectType );
    }

    public CsdlEntitySet getEntitySet( FullQualifiedName entityContainer, String entitySetName ) {
        EntitySet entitySet = dms.getEntitySet( entitySetName );
        return Transformers.transform( entitySet );
    }

    public CsdlEntityContainer getEntityContainer() {
        // create EntitySets
        List<CsdlEntitySet> entitySets = Lists.newArrayList( Iterables
                .filter( Iterables.transform( dms.getEntitySets(), Transformers::transform ), Predicates.notNull() ) );
        // this.entitySets.keySet().forEach( e -> entitySets.add( getEntitySet( CONTAINER, e ) ) );
        // entitySets.add( getEntitySet( CONTAINER, ES_PRODUCTS_NAME ) );

        // create EntityContainer
        CsdlEntityContainer entityContainer = new CsdlEntityContainer();
        entityContainer.setName( CONTAINER_NAME );
        entityContainer.setEntitySets( entitySets );

        return entityContainer;
    }

    public List<CsdlSchema> getSchemas() throws ODataException {
        List<CsdlSchema> schemas = new ArrayList<CsdlSchema>();
        for ( Schema schemaMetadata : dms.getSchemas() ) {
            CsdlSchema schema = new CsdlSchema();
            String namespace = schemaMetadata.getNamespace();
            schema.setNamespace( namespace );
            List<CsdlEntityType> entityTypes = schemaMetadata.getEntityTypeFqns().parallelStream()
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
