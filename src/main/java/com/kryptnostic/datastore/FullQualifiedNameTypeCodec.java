package com.kryptnostic.datastore;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class FullQualifiedNameTypeCodec extends TypeCodec<FullQualifiedName> {

    public FullQualifiedNameTypeCodec() {
        super( DataType.text(), FullQualifiedName.class );
    }

    @Override
    public ByteBuffer serialize( FullQualifiedName value, ProtocolVersion protocolVersion )
            throws InvalidTypeException {
        // byte[] namespaceBytes = value.getNamespace().getBytes( StandardCharsets.UTF_8);
        // byte[] nameBytes = value.getName().getBytes( StandardCharsets.UTF_8 );
        // ByteBuffer buf = ByteBuffer.allocate( (Integer.BYTES<<1) + namespaceBytes.length + nameBytes.length );
        // buf.putInt( namespaceBytes.length );
        // buf.put( namespaceBytes );
        // buf.put( nameBytes.length );
        // buf.put
        if ( value == null ) {
            return null;
        }
        byte[] fqnBytes = value.getFullQualifiedNameAsString().getBytes( StandardCharsets.UTF_8 );
        return ByteBuffer.wrap( fqnBytes );
    }

    @Override
    public FullQualifiedName deserialize( ByteBuffer bytes, ProtocolVersion protocolVersion )
            throws InvalidTypeException {
        if ( bytes == null ) {
            return null;
        }
        // TODO Auto-generated method stub
        byte[] b = new byte[ bytes.remaining() ];
        bytes.duplicate().get( b );

        return new FullQualifiedName( new String( b, StandardCharsets.UTF_8 ) );
    }

    @Override
    public FullQualifiedName parse( String value ) throws InvalidTypeException {
        return new FullQualifiedName( value );
    }

    @Override
    public String format( FullQualifiedName value ) throws InvalidTypeException {
        return value.getFullQualifiedNameAsString();
    }

}
