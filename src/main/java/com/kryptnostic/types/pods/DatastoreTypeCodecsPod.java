package com.kryptnostic.types.pods;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.extras.codecs.enums.EnumNameCodec;

@Configuration
public class DatastoreTypeCodecsPod {
    @Bean
    public EnumNameCodec<EdmPrimitiveTypeKind> edmPrimitiveTypeKindCodec() {
        return new EnumNameCodec<>( EdmPrimitiveTypeKind.class );
    }
}
