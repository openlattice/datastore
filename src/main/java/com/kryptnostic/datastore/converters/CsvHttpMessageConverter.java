package com.kryptnostic.datastore.converters;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractGenericHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.collect.Multimap;
import com.kryptnostic.datastore.constants.CustomMediaType;

public class CsvHttpMessageConverter
        extends AbstractGenericHttpMessageConverter<Iterable<Multimap<FullQualifiedName, ?>>> {

    private final CsvMapper csvMapper = new CsvMapper();

    public CsvHttpMessageConverter() {
        super( CustomMediaType.TEXT_CSV );
        csvMapper.registerModule( new AfterburnerModule() );
        csvMapper.registerModule( new GuavaModule() );
        csvMapper.registerModule( new JodaModule() );
    }

    @Override
    public Iterable<Multimap<FullQualifiedName, ?>> read(
            Type type,
            Class<?> contextClass,
            HttpInputMessage inputMessage )
            throws IOException, HttpMessageNotReadableException {
        throw new UnsupportedOperationException( "CSV is not a supported input format" );
    }

    @Override
    protected void writeInternal(
            Iterable<Multimap<FullQualifiedName, ?>> t,
            Type type,
            HttpOutputMessage outputMessage )
            throws IOException, HttpMessageNotWritableException {
        CsvSchema schema = null;
        // Get schema
        try {
            Multimap<FullQualifiedName, ?> obj = t.iterator().next();
            schema = schemaBuilder( obj );
        } catch ( Exception e ) {
            logger.error( "Cannot build Schema for CSV" );
        }
        // Write to CSV
        csvMapper.writer( schema ).writeValue( outputMessage.getBody(), t );
    }

    @Override
    protected boolean supports( Class<?> clazz ) {
        return Iterable.class.isAssignableFrom( clazz );
    }

    @Override
    protected Iterable<Multimap<FullQualifiedName, ?>> readInternal(
            Class<? extends Iterable<Multimap<FullQualifiedName, ?>>> clazz,
            HttpInputMessage inputMessage ) throws IOException, HttpMessageNotReadableException {
        throw new UnsupportedOperationException( "CSV is not a supported input format" );
    }

    public CsvSchema schemaBuilder( Multimap<FullQualifiedName, ?> obj ) {
        Builder schemaBuilder = CsvSchema.builder();

        // Ignoring Property Multiplicity for now
        for ( FullQualifiedName type : obj.keySet() ) {
            schemaBuilder.addColumn( type.toString(), ColumnType.NUMBER_OR_STRING );
        }
        return schemaBuilder.build();
    }
}
