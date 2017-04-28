/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.datastore.converters;

import java.io.IOException;
import java.lang.reflect.Type;

import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.AbstractGenericHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import com.dataloom.data.EntitySetData;
import com.dataloom.datastore.constants.CustomMediaType;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.ColumnType;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

public class CsvHttpMessageConverter
        extends AbstractGenericHttpMessageConverter<EntitySetData<?>> {

    private final CsvMapper csvMapper = new CsvMapper();

    public CsvHttpMessageConverter() {
        super( CustomMediaType.TEXT_CSV );
        csvMapper.registerModule( new AfterburnerModule() );
        csvMapper.registerModule( new GuavaModule() );
        csvMapper.registerModule( new JodaModule() );
        csvMapper.configure( CsvGenerator.Feature.STRICT_CHECK_FOR_QUOTING, true );
    }

    @Override
    public EntitySetData<?> read(
            Type type,
            Class<?> contextClass,
            HttpInputMessage inputMessage )
            throws IOException, HttpMessageNotReadableException {
        throw new UnsupportedOperationException( "CSV is not a supported input format" );
    }

    @Override
    protected void writeInternal(
            EntitySetData<?> t,
            Type type,
            HttpOutputMessage outputMessage )
            throws IOException, HttpMessageNotWritableException {
        // Get schema
        CsvSchema schema = schemaBuilder( t );
        // Write to CSV
        csvMapper.writer( schema ).writeValue( outputMessage.getBody(), t.getEntities() );
    }

    @Override
    protected boolean supports( Class<?> clazz ) {
        return EntitySetData.class.isAssignableFrom( clazz );
    }

    @Override
    protected EntitySetData<?> readInternal(
            Class<? extends EntitySetData<?>> clazz,
            HttpInputMessage inputMessage ) throws IOException, HttpMessageNotReadableException {
        throw new UnsupportedOperationException( "CSV is not a supported input format" );
    }

    public CsvSchema schemaBuilder( EntitySetData<?> t ) {
        Builder schemaBuilder = CsvSchema.builder();

        for ( Object type : t.getAuthorizedPropertyFqns() ) {
            schemaBuilder.addColumn( type.toString(), ColumnType.ARRAY );
        }
        return schemaBuilder.setUseHeader( true ).build();
    }
}
