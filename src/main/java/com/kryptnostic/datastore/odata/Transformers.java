package com.kryptnostic.datastore.odata;

import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlPropertyRef;

import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.datastore.services.EdmManager;

public final class Transformers {
    private Transformers() {}

    public static final class EntityTypeTransformer {
        private final EdmManager dms;

        public EntityTypeTransformer( EdmManager dms ) {
            this.dms = dms;
        }

        public CsdlEntityType transform( EntityType objectType ) {
            if ( objectType == null ) {
                return null;
            }

            CsdlEntityType entityType = new CsdlEntityType();

            entityType.setName( objectType.getName() );

            entityType.setKey( objectType.getKey().stream()
                    .map( name -> new CsdlPropertyRef().setName( name.getName() ) ).collect( Collectors.toList() ) );
            entityType.setProperties(
                    objectType.getProperties().stream()
                            .map( ( prop ) -> new CsdlProperty().setName( prop.getName() )
                                    .setType(
                                            dms.getPropertyType( prop ).getDatatype().getFullQualifiedName() ) )
                            .collect( Collectors.toList() ) );
            return entityType;
        }

    }

    public static CsdlEntitySet transform( EntitySet entitySet ) {
        if ( entitySet == null ) {
            return null;
        }

        return new CsdlEntitySet()
                .setType( entitySet.getType() )
                .setName( entitySet.getName() );
    }
}
