package com.kryptnostic.datastore.util;

public final class DatastoreConstants {
    private DatastoreConstants() {}

    public static final String KEYSPACE             = "sparks";
    public static final String SCHEMAS_TABLE        = "schemas";
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
            public static final String ACL_ID       = "aId";
        }

        // Table Creation
        public static final String CREATE_SCHEMAS_TABLE                   = "CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + "."
                + SCHEMAS_TABLE
                + " ( aclId uuid, namespace text, name text, entityTypes set<text>, PRIMARY KEY ( aclId, namespace, name ) )";
        public static final String CREATE_ENTITY_SETS_TABLE               = "CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + "."
                + ENTITY_SETS_TABLE
                + " ( type text, name text, title text, PRIMARY KEY ( type, name ) )";
        public static final String CREATE_ENTITY_TYPES_TABLE              = "CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + "."
                + ENTITY_TYPES_TABLE
                + " ( namespace text, type text, typename text, key set<text>, properties set<text>, PRIMARY KEY ( namespace, type ) )";
        public static final String CREATE_PROPERTY_TYPES_TABLE            = "CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + "."
                + PROPERTY_TYPES_TABLE
                + " ( namespace text, type text, typename text, dataType text, multiplicity bigint, PRIMARY KEY ( namespace, type ) )";
        public static final String PROPERTY_TABLE                         = "CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + ".%s_properties ( objectId uuid, aclId uuid, value %s, syncIds list<uuid>, PRIMARY KEY ( ( objectId, aclId ), value ) )";

        // Index creation
        public static final String CREATE_INDEX_ON_NAME                   = "CREATE INDEX IF NOT EXISTS ON " + KEYSPACE
                + "."
                + ENTITY_SETS_TABLE + " (name)";

        // Lightweight transactions for object insertion.
        public static final String CREATE_SCHEMA_IF_NOT_EXISTS            = "INSERT INTO sparks." + SCHEMAS_TABLE
                + " (namespace, name, aclId, entityTypes) VALUES (?,?,?,?) IF NOT EXISTS";
        public static final String CREATE_ENTITY_SET_IF_NOT_EXISTS        = "INSERT INTO sparks." + ENTITY_SETS_TABLE
                + " (type, name, title) VALUES (?,?,?) IF NOT EXISTS";
        public static final String CREATE_ENTITY_TYPE_IF_NOT_EXISTS       = "INSERT INTO sparks." + ENTITY_TYPES_TABLE
                + " (namespace, type, typename, key, properties) VALUES (?,?,?,?,?) IF NOT EXISTS";
        public static final String CREATE_PROPERTY_TYPE_IF_NOT_EXISTS     = "INSERT INTO sparks." + PROPERTY_TYPES_TABLE
                + " (namespace, type, typename, datatype, multiplicity) VALUES (?,?,?,?,?) IF NOT EXISTS";

        // Read queries for datastore.
        public static final String GET_ALL_ENTITY_SETS                    = "select * from sparks."
                + ENTITY_SETS_TABLE;
        public static final String GET_ENTITY_SET_BY_NAME                 = "select * from sparks."
                + ENTITY_SETS_TABLE + " where name = ?";
        public static final String GET_ALL_ENTITY_TYPES_QUERY             = "select * from sparks."
                + ENTITY_TYPES_TABLE;
        public static final String GET_ALL_PROPERTY_TYPES_FOR_ENTITY_TYPE = "select * from sparks."
                + PROPERTY_TYPES_TABLE + " where namespace=:"
                + ParamNames.NAMESPACE + " AND type=:"
                + ParamNames.ENTITY_TYPE;
        public static final String GET_ALL_NAMESPACES                     = "select * from sparks." + SCHEMAS_TABLE
                + " where aclId IN :"
                + ParamNames.ACL_IDS + " ALLOW filtering";
        public static final String ADD_ENTITY_TYPES_TO_SCHEMA             = "UPDATE sparks." + SCHEMAS_TABLE
                + " SET entityTypes = entityTypes + :"
                + ParamNames.ENTITY_TYPES + " where aclId = :" + ParamNames.ACL_ID + " AND namespace = :"
                + ParamNames.NAMESPACE + " AND name = :"
                + ParamNames.NAME;
        public static final String REMOVE_ENTITY_TYPES_FROM_SCHEMA        = "UPDATE sparks." + SCHEMAS_TABLE
                + " SET entityTypes = entityTypes - :"
                + ParamNames.ENTITY_TYPES + " where aclId = :" + ParamNames.ACL_ID + " AND namespace = :"
                + ParamNames.NAMESPACE + " AND name = :"
                + ParamNames.NAME;
    }
}
