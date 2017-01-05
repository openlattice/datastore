package com.kryptnostic.datastore.pods;

import java.util.List;

import javax.inject.Inject;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kryptnostic.datastore.constants.CustomMediaType;
import com.kryptnostic.datastore.constants.DatastoreConstants;
import com.kryptnostic.datastore.converters.CsvHttpMessageConverter;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;

@Configuration
@ComponentScan(
    basePackages = { "com.kryptnostic.datastore.data.controllers", "com.kryptnostic.datastore.edm.controllers",
            "com.kryptnostic.datastore.util", "com.kryptnostic.datastore.permissions.controllers",
            "com.kryptnostic.datastore.directory.controllers" },
    includeFilters = @ComponentScan.Filter(
        value = { org.springframework.stereotype.Controller.class, org.springframework.stereotype.Service.class,
                org.springframework.web.bind.annotation.ControllerAdvice.class },
        type = FilterType.ANNOTATION ) )
@EnableAsync
@EnableMetrics(
    proxyTargetClass = true )
public class DataStoreMvcPod extends WebMvcConfigurationSupport {

    @Inject
    private ObjectMapper defaultObjectMapper;

    @Override
    protected void configureMessageConverters( List<HttpMessageConverter<?>> converters ) {
        super.addDefaultHttpMessageConverters( converters );
        for ( HttpMessageConverter<?> converter : converters ) {
            if ( converter instanceof MappingJackson2HttpMessageConverter ) {
                MappingJackson2HttpMessageConverter jackson2HttpMessageConverter = (MappingJackson2HttpMessageConverter) converter;
                jackson2HttpMessageConverter.setObjectMapper( defaultObjectMapper );
            }
        }
        converters.add( new CsvHttpMessageConverter() );
    }

    // TODO: We need to lock this down. Since all endpoints are stateless + authenticated this is more a
    // defense-in-depth measure.
    @Override
    protected void addCorsMappings( CorsRegistry registry ) {
        registry
                .addMapping( "/**" )
                .allowedMethods( "GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH" )
                .allowedOrigins( "*" );
        super.addCorsMappings( registry );
    }

    @Override
    protected void configureContentNegotiation( ContentNegotiationConfigurer configurer ) {
        configurer.parameterName( DatastoreConstants.FILE_TYPE )
                .favorParameter( true )
                .mediaType( "csv", CustomMediaType.TEXT_CSV )
                .mediaType( "json", MediaType.APPLICATION_JSON )
                .defaultContentType( MediaType.APPLICATION_JSON );
    }
}
