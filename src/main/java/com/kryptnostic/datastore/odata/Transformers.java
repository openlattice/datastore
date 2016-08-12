package com.kryptnostic.datastore.odata;

import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlPropertyRef;

import com.kryptnostic.types.EntitySet;
import com.kryptnostic.types.EntityType;

public final class Transformers {
    private Transformers() {}

    public static CsdlEntityType transform( EntityType objectType ) {
        CsdlEntityType entityType = new CsdlEntityType();

        entityType.setName( objectType.getType() );

        entityType.setKey( objectType.getKey().stream()
                .map( name -> new CsdlPropertyRef().setName( name ) ).collect( Collectors.toList() ) );
        entityType.setProperties(
                objectType.getAllowed().stream()
                        .map( ( prop ) -> new CsdlProperty().setName( prop )
                                .setType( new FullQualifiedName( objectType.getNamespace(), prop ) ) )
                        .collect( Collectors.toList() ) );
        return entityType;
    }

    public static CsdlEntitySet transform( EntitySet entitySet ) {
        if( entitySet == null ) {
            return null;
        }
        
        return new CsdlEntitySet()
                .setType( new FullQualifiedName( entitySet.getNamespace(), entitySet.getType() ) )
                .setName( entitySet.getName() );
    }
}
