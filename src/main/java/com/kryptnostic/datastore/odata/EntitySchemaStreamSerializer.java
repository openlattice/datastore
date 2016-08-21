package com.kryptnostic.datastore.odata;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

import com.google.common.collect.Maps;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.kryptnostic.datastore.odata.Ontology.EntitySchema;
import com.kryptnostic.mapstores.v1.constants.HazelcastSerializerTypeIds;
import com.kryptnostic.rhizome.hazelcast.serializers.StreamSerializerUtils;
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer;

/**
 *  
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt; 
 * Serializes entity schema for type service.
 * TODO: Move into mapstores.
 *
 */
public class EntitySchemaStreamSerializer implements SelfRegisteringStreamSerializer<EntitySchema> {
    private static final EdmPrimitiveTypeKind[] primitiveTypeKinds = EdmPrimitiveTypeKind.values();

    @Override
    public int getTypeId() {
        return HazelcastSerializerTypeIds.ENTITY_SCHEMA.ordinal();
    }

    @Override
    public void destroy() {

    }

    @Override
    public void write( ObjectDataOutput out, EntitySchema object ) throws IOException {
        StreamSerializerUtils.writeStringList( out, object.getKeyProperties() );
        serializeEntityMap( out, object.getProperties() );
    }

    @Override
    public EntitySchema read( ObjectDataInput in ) throws IOException {
        List<String> keyProperties = StreamSerializerUtils.readStringArrayList( in );
        Map<String, EdmPrimitiveTypeKind> propertyTypes = deserializePropertyTypes( in );
        return new EntitySchema( propertyTypes, keyProperties );
    }

    @Override
    public Class<EntitySchema> getClazz() {
        return EntitySchema.class;
    }
    
    public static void serializeEntityMap( ObjectDataOutput out, Map<String, EdmPrimitiveTypeKind> m )
            throws IOException {
        out.writeInt( m.size() );
        for ( Entry<String, EdmPrimitiveTypeKind> entry : m.entrySet() ) {
            out.writeUTF( entry.getKey() );
            out.writeInt( entry.getValue().ordinal() );
        }
    }

    public static Map<String, EdmPrimitiveTypeKind> deserializePropertyTypes( ObjectDataInput in ) throws IOException {
        int size = in.readInt();
        Map<String, EdmPrimitiveTypeKind> m = Maps.newHashMapWithExpectedSize( size );
        for ( int i = 0; i < size; ++i ) {
            String propertyName = in.readUTF();
            EdmPrimitiveTypeKind propertyType = primitiveTypeKinds[ in.readInt() ];
            m.put( propertyName, propertyType );
        }
        return m;
    }


}
