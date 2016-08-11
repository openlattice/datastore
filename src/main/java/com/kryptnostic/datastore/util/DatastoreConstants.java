package com.kryptnostic.datastore.util;

public final class DatastoreConstants {
    private DatastoreConstants() {}

    public static final String KEYSPACE             = "sparks";
    public static final String NAMESPACE_TABLE      = "namespaces";
    public static final String CONTAINERS_TABLE     = "containers";
    public static final String OBJECT_TYPES_TABLE   = "object_types";
    public static final String PROPERTY_TYPES_TABLE = "property_types";
    public static final String PRIMARY_NAMESPACE    = "agora";

    public static final class Queries {
        public static final class ParamNames {
            public static final String OBJECT_ID = "objId";
            public static final String ACL_IDS   = "aclIds";
        }

        public static final String GET_ALL_OBJECT_TYPES_QUERY        = "select * from sparks.object_types";
        public static final String GET_ALL_PROPERTY_TYPES_FOR_OBJECT = "select * from sparks.property_types where objectId=:"
                + ParamNames.OBJECT_ID;
        public static final String GET_ALL_NAMESPACES                = "select * from sparks.namespaces where aclId IN :"
                + ParamNames.ACL_IDS + " ALLOW filtering";
        public static final String CREATE_NAMESPACE_TABLE            = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "."
                + NAMESPACE_TABLE + " ( namespace text, aclId uuid, PRIMARY KEY ( namespace, aclid ) )";
        public static final String CREATE_OBJECT_TYPES_TABLE         = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "."
                + OBJECT_TYPES_TABLE
                + " ( namespace text, type text, typename text, keys set<text>,allowed set<text>, PRIMARY KEY ( ( namespace,type), typename) )";
        public static final String CREATE_PROPERTY_TYPES_TABLE       = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "."
                + PROPERTY_TYPES_TABLE
                + " ( namespace text, type text, typename text, dataType text, multiplicity bigint, PRIMARY KEY ( ( namespace,type), typename) )";
        public static final String PROPERTY_TABLE                    = "CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + ".%s_properties ( objectId uuid, aclId uuid, value %s, syncIds list<uuid>, PRIMARY KEY ( ( objectId, aclId ), value ) )";

    }
}
