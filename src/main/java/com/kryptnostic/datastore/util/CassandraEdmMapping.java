package com.kryptnostic.datastore.util;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

import com.datastax.driver.core.DataType;

public class CassandraEdmMapping {
    public static String getCassandraTypeName( EdmPrimitiveTypeKind edmPrimitveTypeKind ) {
        return getCassandraType( edmPrimitveTypeKind ).getName().name();
    }
    
    //TODO: Consider replacing with EnumMap?
    public static DataType getCassandraType( EdmPrimitiveTypeKind edmPrimitveTypeKind ) {
        switch ( edmPrimitveTypeKind ) {
            case Binary:
                return DataType.blob();
            case Boolean:
                return DataType.cboolean();
            case Byte:
                // This will require special processing :-/
                return DataType.blob();
            case Date:
                return DataType.date();
            case DateTimeOffset:
                return DataType.timestamp();
            case Decimal:
                return DataType.decimal();
            case Double:
                return DataType.cdouble();
            case Duration:
                // There be dragons. Parsing format for durations looks terrible. Currently just storing string and
                // hoping for the best.
                return DataType.text();
            case Guid:
                return DataType.uuid();
            case Int16:
                return DataType.smallint();
            case Int32:
                return DataType.cint();
            case Int64:
                return DataType.bigint();
            case String:
                return DataType.text();
            case SByte:
                return DataType.tinyint();
            case Single:
                return DataType.cfloat();
            case TimeOfDay:
                return DataType.time();

            default:
                return DataType.blob();
        }
    }
}
