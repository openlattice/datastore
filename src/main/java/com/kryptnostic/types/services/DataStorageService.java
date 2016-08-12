package com.kryptnostic.types.services;

import java.util.List;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriParameter;

public class DataStorageService {

//    private final IMap<String, FullQualifiedName>       entitySets;
//    private final IMap<FullQualifiedName, EntitySchema> entitySchemas;
    
    public EntityCollection readEntitySetData(EdmEntitySet edmEntitySet)throws ODataApplicationException{

        // actually, this is only required if we have more than one Entity Sets
//        if(edmEntitySet.getName().equals(DemoEdmProvider.ES_PRODUCTS_NAME)){
//            return getProducts();
//        }

        //TODO: RPC to Spark to load data.
        return null;
    }

    public Entity readEntityData(EdmEntitySet edmEntitySet, List<UriParameter> keyParams) throws ODataApplicationException{

        EdmEntityType edmEntityType = edmEntitySet.getEntityType();

        // actually, this is only required if we have more than one Entity Type
//        if(edmEntityType.getName().equals(DemoEdmProvider.ET_PRODUCT_NAME)){
////            return getProduct(edmEntityType, keyParams);
//        }

        return null;
    }

    public Entity createEntityData( EdmEntitySet edmEntitySet, Entity requestEntity ) {
        // edmEntitySet.getEntityType().getKeyPropertyRefs().forEach( kp -> kp.getProperty().getType(). );
        // TODO Auto-generated method stub
        
        return null;
    }

    
}
