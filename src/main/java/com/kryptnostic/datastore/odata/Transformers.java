package com.kryptnostic.datastore.odata;

import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlPropertyRef;

import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
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

            entityType.setName( objectType.getType().getName() );

            entityType.setKey( objectType.getKey().stream()
                    .map( dms::getPropertyType )
                    .map( k -> new CsdlPropertyRef().setName( k.getType().getName() ) )
                    .collect( Collectors.toList() ) );
            entityType.setProperties(
                    objectType.getProperties().stream()
                            .map( dms::getPropertyType )
                            .map( ( prop ) -> new CsdlProperty().setName( prop.getType().getName() )
                                    .setType( prop.getType() ) )
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
