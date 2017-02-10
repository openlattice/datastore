package com.dataloom.datastore.converters;

import java.io.IOException;
import java.lang.reflect.Type;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.AbstractGenericHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import com.dataloom.datastore.constants.CustomMediaType;
import com.dataloom.mappers.ObjectMappers;
import com.google.common.collect.Multimap;

public class YamlHttpMessageConverter extends AbstractGenericHttpMessageConverter<Object> {

    private final ObjectMapper mapper = ObjectMappers.getYamlMapper();
    
    public YamlHttpMessageConverter() {
        super( CustomMediaType.TEXT_YAML );
    }
    
    @Override
    public Iterable<Multimap<FullQualifiedName, ?>> read(
            Type type,
            Class<?> contextClass,
            HttpInputMessage inputMessage ) throws IOException, HttpMessageNotReadableException {
        throw new UnsupportedOperationException( "YAML is not a supported input format" );
    }

    @Override
    protected void writeInternal(
            Object t,
            Type type,
            HttpOutputMessage outputMessage ) throws IOException, HttpMessageNotWritableException {
        mapper.writeValue( outputMessage.getBody(), t );
        
    }

    @Override
    protected boolean supports( Class<?> clazz ) {
        return Object.class.isAssignableFrom( clazz );
    }

    @Override
    protected Object readInternal(
            Class<? extends Object> clazz,
            HttpInputMessage inputMessage ) throws IOException, HttpMessageNotReadableException {
        throw new UnsupportedOperationException( "YAML is not a supported input format" );
    }

}
