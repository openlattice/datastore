package com.kryptnostic.datastore.util;

public final class DatastoreConstants {
    private DatastoreConstants() {}

    public static final String KEYSPACE             = "sparks";
    public static final String SCHEMA_TABLE         = "schemas";
    public static final String CONTAINERS_TABLE     = "containers";
    public static final String ENTITY_SETS_TABLE    = "entity_sets";
    public static final String ENTITY_TYPES_TABLE   = "entity_types";
    public static final String PROPERTY_TYPES_TABLE = "property_types";
    public static final String PRIMARY_NAMESPACE    = "agora";
    public static final String APPLIED_FIELD        = "[applied]";

    public static final class Queries {
        public static final class ParamNames {
            public static final String ENTITY_TYPE  = "entType";
            public static final String ACL_IDS      = "aclIds";
            public static final String NAMESPACE    = "namespace";
            public static final String NAME         = "name";
            public static final String ENTITY_TYPES = "entTypes";
            public static final String ACL_ID       = "aclId";
        }

        // Table Creation
        public static final String CREATE_NAMESPACE_TABLE                 = "CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + "."
                + SCHEMA_TABLE
                + " ( namespace text, aclId uuid, container text, entityTypes set<text>, PRIMARY KEY ( namespace, aclid ) )";
        public static final String CREATE_ENTITY_SET_TABLE                = "CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + "."
                + ENTITY_SETS_TABLE
                + " ( namespace text, container text, name text, typename text, PRIMARY KEY ( ( namespace, container ) , name ) )";
        public static final String CREATE_ENTITY_TYPES_TABLE              = "CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + "."
                + ENTITY_TYPES_TABLE
                + " ( namespace text, type text, typename text, keys set<text>,allowed set<text>, PRIMARY KEY ( ( namespace,type), typename) )";
        public static final String CREATE_PROPERTY_TYPES_TABLE            = "CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + "."
                + PROPERTY_TYPES_TABLE
                + " ( namespace text, type text, typename text, dataType text, multiplicity bigint, PRIMARY KEY ( ( namespace,type), typename) )";
        public static final String PROPERTY_TABLE                         = "CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + ".%s_properties ( objectId uuid, aclId uuid, value %s, syncIds list<uuid>, PRIMARY KEY ( ( objectId, aclId ), value ) )";

        // Index creation
        public static final String CREATE_INDEX_ON_TYPENAME               = "CREATE INDEX IF NOT EXISTS " + KEYSPACE
                + "."
                + ENTITY_SETS_TABLE + " (typename)";
        // Lightweight transactions for object insertion.
        public static final String CREATE_SCHEMA_IF_NOT_EXISTS            = "INSERT INTO sparks." + SCHEMA_TABLE
                + " (namespace, aclId, container, entityTypes) VALUES (?,?,?,?) IF NOT EXISTS";
        public static final String CREATE_ENTITY_TYPE_IF_NOT_EXISTS       = "INSERT INTO sparks." + ENTITY_TYPES_TABLE
                + " (namespace, type, typename, keys, allowed) VALUES (?,?,?,?,?) IF NOT EXISTS";
        public static final String CREATE_PROPERTY_TYPE_IF_NOT_EXISTS     = "INSERT INTO sparks." + PROPERTY_TYPES_TABLE
                + " (namespace, type, typename, datatype, multiplicity) VALUES (?,?,?,?,?) IF NOT EXISTS";

        // Read queries for datastore.
        public static final String GET_ALL_ENTITY_TYPES_QUERY             = "select * from sparks."
                + ENTITY_TYPES_TABLE;
        public static final String GET_ALL_PROPERTY_TYPES_FOR_ENTITY_TYPE = "select * from sparks."
                + PROPERTY_TYPES_TABLE + " where namespace=:"
                + ParamNames.NAMESPACE + " AND type=:"
                + ParamNames.ENTITY_TYPE;
        public static final String GET_ALL_NAMESPACES                     = "select * from sparks.namespaces where aclId IN :"
                + ParamNames.ACL_IDS + " ALLOW filtering";
        public static final String ADD_ENTITY_TYPES_TO_CONTAINER          = "UPDATE sparks." + SCHEMA_TABLE
                + " SET entityTypes = entityTypes + :"
                + ParamNames.ENTITY_TYPES + " where namespace = :" + ParamNames.NAMESPACE + " AND container = :"
                + ParamNames.NAME;
        public static final String REMOVE_ENTITY_TYPES_FROM_CONTAINER     = "UPDATE sparks." + SCHEMA_TABLE
                + " SET entityTypes = entityTypes - :"
                + ParamNames.ENTITY_TYPES + " where namespace = :" + ParamNames.NAMESPACE + " AND container = :"
                + ParamNames.NAME;
    }
}
