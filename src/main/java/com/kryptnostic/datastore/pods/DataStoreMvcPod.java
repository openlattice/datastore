package com.kryptnostic.datastore.pods;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import javax.inject.Inject;
import java.util.List;

/**
 * Created by yao on 9/20/16.
 */
@Configuration
@ComponentScan(
        basePackages = { "com.kryptnostic.datastore.data.controllers", "com.kryptnostic.datastore.edm.controllers",
                "com.kryptnostic.datastore.odata.controllers", "com.kryptnostic.datastore.util" },
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
    }
}
