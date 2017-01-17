package com.dataloom.datastore.odata;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

import com.dataloom.datastore.odata.Transformers.EntityTypeTransformer;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.Schema;
import com.dataloom.edm.schemas.manager.HazelcastSchemaManager;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.kryptnostic.datastore.services.EdmManager;

import jersey.repackaged.com.google.common.collect.Lists;

/**
 * Loom Entity Data Model provider.
 * 
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 *
 */
public class LoomEdmProvider extends CsdlAbstractEdmProvider {
    private static final Logger          logger         = LoggerFactory
            .getLogger( LoomEdmProvider.class );
    public static final String           NAMESPACE      = "OData.Demo";
    public final String                  CONTAINER_NAME = "Container";
    public final FullQualifiedName       CONTAINER      = new FullQualifiedName(
            NAMESPACE,
            CONTAINER_NAME );
    private final EdmManager             dms;
    private final HazelcastSchemaManager schemaManager;
    private final EntityTypeTransformer  ett;

    public LoomEdmProvider( EdmManager dms, HazelcastSchemaManager schemaManager ) {
        this.dms = dms;
        this.schemaManager = schemaManager;
        this.ett = new EntityTypeTransformer( dms );
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

        // create EntityContainer
        CsdlEntityContainer entityContainer = new CsdlEntityContainer();
        entityContainer.setName( CONTAINER_NAME );
        entityContainer.setEntitySets( entitySets );

        return entityContainer;
    }

    public List<CsdlSchema> getSchemas() throws ODataException {
        List<CsdlSchema> schemas = new ArrayList<CsdlSchema>();

        for ( Schema schemaMetadata : schemaManager.getAllSchemas() ) {
            CsdlSchema schema = new CsdlSchema();
            String namespace = schemaMetadata.getFqn().getNamespace();
            schema.setNamespace( namespace );
            List<CsdlEntityType> entityTypes = schemaMetadata.getEntityTypes().parallelStream()
                    .map( fqn -> {
                        try {
                            return getEntityType( fqn.getType() );
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
