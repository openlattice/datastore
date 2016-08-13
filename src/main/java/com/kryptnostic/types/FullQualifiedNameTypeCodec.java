package com.kryptnostic.types;

import java.nio.ByteBuffer;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class FullQualifiedNameTypeCodec extends TypeCodec<FullQualifiedName> {

    @Override
    public ByteBuffer serialize( FullQualifiedName value, ProtocolVersion protocolVersion )
            throws InvalidTypeException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FullQualifiedName deserialize( ByteBuffer bytes, ProtocolVersion protocolVersion )
            throws InvalidTypeException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FullQualifiedName parse( String value ) throws InvalidTypeException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String format( FullQualifiedName value ) throws InvalidTypeException {
        // TODO Auto-generated method stub
        return null;
    }

}
