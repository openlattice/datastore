package com.kryptnostic.types.pods;

import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.enums.EnumNameCodec;

@Configuration
public class DatastoreTypeCodecsPod {
    @Bean
    public EnumNameCodec<EdmPrimitiveTypeKind> edmPrimitiveTypeKindCodec() {
        return new EnumNameCodec<>( EdmPrimitiveTypeKind.class );
    }

    @Bean
    public TypeCodec<Set<String>> setStringCodec() {
        return TypeCodec.set( TypeCodec.varchar() );
    }

    @Bean 
    public TypeCodec<Set<UUID>> setUuidCodec() {
        return TypeCodec.set( TypeCodec.uuid() );
    }
    
}
