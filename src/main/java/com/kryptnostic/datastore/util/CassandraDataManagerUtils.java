package com.kryptnostic.datastore.util;

import java.nio.ByteBuffer;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CassandraDataManagerUtils {

    private ObjectMapper                 mapper;
    private static final ProtocolVersion protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;

    public CassandraDataManagerUtils( ObjectMapper mapper ) {
        this.mapper = mapper;
    }

    /**
     * This directly depends on Jackson's raw data binding. See http://wiki.fasterxml.com/JacksonInFiveMinutes
     * 
     * @param type
     * @return
     * @throws JsonProcessingException
     */
    public ByteBuffer serialize( Object value, EdmPrimitiveTypeKind type )
            throws JsonProcessingException {
        switch ( type ) {
            // To come back to: binary, byte
            /**
             * Jackson binds to Boolean
             */
            case Boolean:
                return TypeCodec.cboolean().serialize( (Boolean) value, protocolVersion );
            /**
             * Jackson binds to String
             */
            case Date:
            case DateTimeOffset:
            case TimeOfDay:
            case Guid:
            case String:
                return TypeCodec.varchar().serialize( (String) value, protocolVersion );
            /**
             * Jackson binds to Double
             */
            case Decimal:
            case Duration:
            case Double:
            case Single:
                return TypeCodec.cdouble().serialize( (Double) value, protocolVersion );
            /**
             * Jackson binds to Integer, Long, or BigInteger
             */
            case SByte:
                return TypeCodec.tinyInt().serialize( ( (Number) value ).byteValue(), protocolVersion );
            case Int16:
                return TypeCodec.smallInt().serialize( ( (Number) value ).shortValue(), protocolVersion );
            case Int32:
                return TypeCodec.cint().serialize( ( (Number) value ).intValue(), protocolVersion );
            case Int64:
                return TypeCodec.bigint().serialize( ( (Number) value ).longValue(), protocolVersion );
            // TODO geospatial points
            default:
                return ByteBuffer.wrap( mapper.writeValueAsBytes( value ) );
        }
    }
}
