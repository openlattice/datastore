package com.kryptnostic.types.pods;

import java.io.IOException;

import javax.inject.Inject;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.rhizome.registries.ObjectMapperRegistry;
import com.kryptnostic.types.services.TypesService;

@Configuration
public class TypesServicesPod {

    @Inject
    private HazelcastInstance hazelcastInstance;

    @Bean
    public ObjectMapper defaultObjectMapper() {
        return ObjectMapperRegistry.getJsonMapper();
    }

    @Bean
    public TypesService typesService() throws IOException {
        return new TypesService( hazelcastInstance );
    }
}
