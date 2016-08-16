package com.kryptnostic.types.pods;

import java.util.List;

import javax.inject.Inject;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kryptnostic.datastore.edm.controllers.EdmController;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;

@Configuration
@ComponentScan(
    basePackageClasses = EdmController.class,
    includeFilters = @ComponentScan.Filter(
        value = { org.springframework.stereotype.Controller.class },
        type = FilterType.ANNOTATION ) )
@EnableMetrics(
    proxyTargetClass = true )
public class EdmMvcPod extends WebMvcConfigurationSupport {
    @Inject
    private ObjectMapper defaultObjectMapper;

    @Override
    protected void configureMessageConverters( List<HttpMessageConverter<?>> converters ) {
        super.addDefaultHttpMessageConverters( converters );
        for ( HttpMessageConverter<?> converter : converters ) {
            if ( converter instanceof MappingJackson2HttpMessageConverter ) {
                MappingJackson2HttpMessageConverter jacksonConverter = (MappingJackson2HttpMessageConverter) converter;
                jacksonConverter.setObjectMapper( defaultObjectMapper );
            }
        }
    }

}
