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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.conductor.rpc.odata.Schema;
import com.kryptnostic.datastore.odata.Transformers.EntityTypeTransformer;
import com.kryptnostic.types.services.EdmManager;

import jersey.repackaged.com.google.common.collect.Lists;

public class KryptnosticEdmProvider extends CsdlAbstractEdmProvider {
    private static final Logger           logger           = LoggerFactory
            .getLogger( KryptnosticEdmProvider.class );
    // Service Namespace
    public static final String            NAMESPACE        = "OData.Demo";

    // EDM Container
    public static final String            CONTAINER_NAME   = "Container";
    public static final FullQualifiedName CONTAINER        = new FullQualifiedName(
            NAMESPACE,
            CONTAINER_NAME );

    // Entity Types Names
    public static final String            ET_PRODUCT_NAME  = "Product";
    public static final FullQualifiedName ET_PRODUCT_FQN   = new FullQualifiedName(
            NAMESPACE,
            ET_PRODUCT_NAME );

    // Entity Set Names
    public static final String            ES_PRODUCTS_NAME = "Products";

    private final EdmManager              dms;
    private final EntityTypeTransformer   ett;

    public KryptnosticEdmProvider( EdmManager dms ) {
        this.dms = dms;
        this.ett = new EntityTypeTransformer( dms );

        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setType( "ID" )
                .setDatatype( EdmPrimitiveTypeKind.Int32 ).setMultiplicity( 0 ) );
        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setType( "Name" )
                .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 ) );
        dms.createPropertyType( new PropertyType().setNamespace( NAMESPACE ).setType( "Description" )
                .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 ) );
        EntityType product = new EntityType().setNamespace( NAMESPACE ).setType( ET_PRODUCT_NAME )
                .setKey( ImmutableSet.of( new FullQualifiedName( NAMESPACE, "ID" ) ) )
                .setProperties( ImmutableSet.of( new FullQualifiedName( NAMESPACE, "ID" ),
                        new FullQualifiedName( NAMESPACE, "Name" ),
                        new FullQualifiedName( NAMESPACE, "Description" ) ) )
                .setTypename( ET_PRODUCT_NAME );

        dms.createEntityType( product );
        dms.createEntitySet( ET_PRODUCT_FQN, ES_PRODUCTS_NAME, null );
        dms.createEntitySet( new FullQualifiedName( NAMESPACE, "metadataLevel" ), "metadataLevels", null );

        dms.createSchema( NAMESPACE,
                "agora",
                ACLs.EVERYONE_ACL,
                ImmutableSet.of( new FullQualifiedName( NAMESPACE, product.getType() ) ) );
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
