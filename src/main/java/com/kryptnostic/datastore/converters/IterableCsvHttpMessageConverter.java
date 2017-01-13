package com.kryptnostic.datastore.converters;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.concurrent.ConcurrentMap;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
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
import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.kryptnostic.datastore.services.EdmService;

import jersey.repackaged.com.google.common.collect.Maps;

public class IterableCsvHttpMessageConverter
        extends AbstractGenericHttpMessageConverter<Iterable<Multimap<String, ?>>> {

    private final CsvMapper                      csvMapper    = new CsvMapper();
    private final ConcurrentMap<String, Boolean> multiplicity = Maps.newConcurrentMap();
    private final EdmService                     edmService;

    public IterableCsvHttpMessageConverter( EdmService edmService ) {
        this.edmService = edmService;
        csvMapper.registerModule( new AfterburnerModule() );
        csvMapper.registerModule( new GuavaModule() );
        csvMapper.registerModule( new JodaModule() );
    }

    @Override
    public Iterable<Multimap<String, ?>> read( Type type, Class<?> contextClass, HttpInputMessage inputMessage )
            throws IOException, HttpMessageNotReadableException {
        throw new UnsupportedOperationException( "CSV is not a supported input format" );
    }

    @Override
    protected void writeInternal( Iterable<Multimap<String, ?>> t, Type type, HttpOutputMessage outputMessage )
            throws IOException, HttpMessageNotWritableException {
        CsvSchema schema = null;

        for ( Multimap<String, ?> obj : t ) {
            if ( schema == null ) {
                schema = fromMultimap( obj );
            }
            // TODO: Flatten or drop multiple output values into obj.
            csvMapper.writer( schema ).writeValue( outputMessage.getBody(), obj.asMap() );
        }

    }

    @Override
    protected boolean supports( Class<?> clazz ) {
        return Iterable.class.isAssignableFrom( clazz );
    }

    @Override
    protected Iterable<Multimap<String, ?>> readInternal(
            Class<? extends Iterable<Multimap<String, ?>>> clazz,
            HttpInputMessage inputMessage )
            throws IOException, HttpMessageNotReadableException {
        throw new UnsupportedOperationException( "CSV is not a supported input format" );
    }

    public CsvSchema fromMultimap( Multimap<String, ?> m ) {
        Builder schemaBuilder = CsvSchema.builder();

        m.keySet().stream().forEach( type -> schemaBuilder.addColumn( type, ColumnType.ARRAY ) );

        return schemaBuilder.build();
    }

}
