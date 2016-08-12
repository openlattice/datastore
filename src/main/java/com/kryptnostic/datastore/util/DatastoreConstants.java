package com.kryptnostic.datastore.util;

public final class DatastoreConstants {
    private DatastoreConstants() {}

    public static final String KEYSPACE             = "sparks";
    public static final String NAMESPACE_TABLE      = "namespaces";
    public static final String CONTAINERS_TABLE     = "containers";
    public static final String OBJECT_TYPES_TABLE   = "object_types";
    public static final String PROPERTY_TYPES_TABLE = "property_types";
    public static final String PRIMARY_NAMESPACE    = "agora";
    public static final String APPLIED_FIELD        = "[applied]";

    public static final class Queries {
        public static final class ParamNames {
            public static final String OBJECT_TYPE  = "objType";
            public static final String ACL_IDS      = "aclIds";
            public static final String NAMESPACE    = "namespace";
            public static final String CONTAINER    = "container";
            public static final String OBJECT_TYPES = "objTypes";
        }

        // Table Creation
        public static final String CREATE_NAMESPACE_TABLE                 = "CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + "."
                + NAMESPACE_TABLE + " ( namespace text, aclId uuid, PRIMARY KEY ( namespace, aclid ) )";
        public static final String CREATE_CONTAINER_TABLE                 = "CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + "."
                + CONTAINERS_TABLE
                + " ( namespace text, container text, objectTypes set<text>, PRIMARY KEY ( namespace, container ) )";
        public static final String CREATE_OBJECT_TYPES_TABLE              = "CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + "."
                + OBJECT_TYPES_TABLE
                + " ( namespace text, type text, typename text, keys set<text>,allowed set<text>, PRIMARY KEY ( ( namespace,type), typename) )";
        public static final String CREATE_PROPERTY_TYPES_TABLE            = "CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + "."
                + PROPERTY_TYPES_TABLE
                + " ( namespace text, type text, typename text, dataType text, multiplicity bigint, PRIMARY KEY ( ( namespace,type), typename) )";
        public static final String PROPERTY_TABLE                         = "CREATE TABLE IF NOT EXISTS " + KEYSPACE
                + ".%s_properties ( objectId uuid, aclId uuid, value %s, syncIds list<uuid>, PRIMARY KEY ( ( objectId, aclId ), value ) )";

        // Lightweight transactions for object insertion.
        public static final String CREATE_NAMESPACE_IF_NOT_EXISTS         = "INSERT INTO sparks.namespaces (namespace, aclId ) VALUES (?,?) IF NOT EXISTS";
        public static final String CREATE_CONTAINER_IF_NOT_EXISTS         = "INSERT INTO sparks.containers ( namespace, container, objectTypes ) VALUES (?,?,?) IF NOT EXISTS";
        public static final String CREATE_OBJECT_TYPE_IF_NOT_EXISTS       = "INSERT INTO sparks.object_types (namespace, type, typename, keys, allowed) VALUES (?,?,?,?,?) IF NOT EXISTS";
        public static final String CREATE_PROPERTY_TYPE_IF_NOT_EXISTS     = "INSERT INTO sparks.property_types (namespace, type, typename, datatype, multiplicity) VALUES (?,?,?,?,?) IF NOT EXISTS";

        // Read queries for datastore.
        public static final String GET_ALL_OBJECT_TYPES_QUERY             = "select * from sparks.object_types";
        public static final String GET_ALL_PROPERTY_TYPES_FOR_OBJECT_TYPE = "select * from sparks.property_types where namespace=:"
                + ParamNames.NAMESPACE + " AND type=:"
                + ParamNames.OBJECT_TYPE;
        public static final String GET_ALL_NAMESPACES                     = "select * from sparks.namespaces where aclId IN :"
                + ParamNames.ACL_IDS + " ALLOW filtering";
        public static final String ADD_OBJECT_TYPES_TO_CONTAINER          = "UPDATE sparks.containers SET objectTypes = objectTypes + :"
                + ParamNames.OBJECT_TYPES + " where namespace = :" + ParamNames.NAMESPACE + " AND container = :"
                + ParamNames.CONTAINER;
        public static final String REMOVE_OBJECT_TYPES_TO_CONTAINER       = "UPDATE sparks.containers SET objectTypes = objectTypes - :"
                + ParamNames.OBJECT_TYPES + " where namespace = :" + ParamNames.NAMESPACE + " AND container = :"
                + ParamNames.CONTAINER;
    }
}
