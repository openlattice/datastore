package com.kryptnostic.datastore.edm;

import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;

public final class Queries {
    private Queries() {}

    public static final class ParamNames {
        public static final String ENTITY_TYPE  = "entType";
        public static final String ACL_IDS      = "aclIds";
        public static final String NAMESPACE    = "namespace";
        public static final String NAME         = "name";
        public static final String ENTITY_TYPES = "entTypes";
        public static final String ACL_ID       = "aId";
        public static final String OBJ_ID       = "objId";
        public static final String ENTITY_SETS  = "entSets";
        public static final String SYNC_IDS     = "sId";
    }

    // Keyspace setup
    public static final String CREATE_KEYSPACE                     = "CREATE KEYSPACE IF NOT EXISTS sparks WITH REPLICATION={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND DURABLE_WRITES=true";
    // Table Creation
    public static final String CREATE_SCHEMAS_TABLE                = "CREATE TABLE IF NOT EXISTS "
            + DatastoreConstants.KEYSPACE
            + "."
            + DatastoreConstants.SCHEMAS_TABLE
            + " ( aclId uuid, namespace text, name text, entityTypeFqns set<text>, PRIMARY KEY ( aclId, namespace, name ) )";
    public static final String CREATE_ENTITY_SETS_TABLE            = "CREATE TABLE IF NOT EXISTS "
            + DatastoreConstants.KEYSPACE
            + "."
            + DatastoreConstants.ENTITY_SETS_TABLE
            + " ( type text, name text, title text, PRIMARY KEY ( type, name ) )";
    public static final String CREATE_ENTITY_TYPES_TABLE           = "CREATE TABLE IF NOT EXISTS "
            + DatastoreConstants.KEYSPACE
            + "."
            + DatastoreConstants.ENTITY_TYPES_TABLE
            + " ( namespace text, name text, typename text, key set<text>, properties set<text>, PRIMARY KEY ( namespace, name ) )";
    public static final String CREATE_PROPERTY_TYPES_TABLE         = "CREATE TABLE IF NOT EXISTS "
            + DatastoreConstants.KEYSPACE
            + "."
            + DatastoreConstants.PROPERTY_TYPES_TABLE
            + " ( namespace text, name text, typename text, dataType text, multiplicity bigint, PRIMARY KEY ( namespace, name ) )";
    public static final String CREATE_PROPERTY_TABLE               = "CREATE TABLE IF NOT EXISTS %s.%s ( objectId uuid, aclId uuid, value %s, syncIds list<uuid>, PRIMARY KEY ( ( objectId, aclId ), value ) )";

    public static final String CREATE_ENTITY_TABLE                 = "CREATE TABLE IF NOT EXISTS %s.%s_entities ( objectId uuid, aclId uuid, clock timestamp, entitySets set<text>, syncIds list<uuid>, PRIMARY KEY ( ( objectId, aclId ), clock ) )";

    // Index creation
    public static final String CREATE_INDEX_ON_NAME                = "CREATE INDEX IF NOT EXISTS ON "
            + DatastoreConstants.KEYSPACE
            + "."
            + DatastoreConstants.ENTITY_SETS_TABLE + " (name)";
    /**
     * This is the query for adding the secondary index on the entitySets column for entity table of a given type
     */
    public static final String CREATE_INDEX_ON_ENTITY_ENTITY_SETS  = "CREATE INDEX IF NOT EXISTS ON %s.%s (entitysets)";

    // Lightweight transactions for object insertion.
    public static final String CREATE_SCHEMA_IF_NOT_EXISTS         = "INSERT INTO sparks."
            + DatastoreConstants.SCHEMAS_TABLE
            + " (namespace, name, aclId, entityTypeFqns) VALUES (?,?,?,?) IF NOT EXISTS";
    public static final String CREATE_ENTITY_SET_IF_NOT_EXISTS     = "INSERT INTO sparks."
            + DatastoreConstants.ENTITY_SETS_TABLE
            + " (type, name, title) VALUES (?,?,?) IF NOT EXISTS";
    public static final String CREATE_ENTITY_TYPE_IF_NOT_EXISTS    = "INSERT INTO sparks."
            + DatastoreConstants.ENTITY_TYPES_TABLE
            + " (namespace, name, typename, key, properties) VALUES (?,?,?,?,?) IF NOT EXISTS";
    public static final String CREATE_PROPERTY_TYPE_IF_NOT_EXISTS  = "INSERT INTO sparks."
            + DatastoreConstants.PROPERTY_TYPES_TABLE
            + " (namespace, name, typename, datatype, multiplicity) VALUES (?,?,?,?,?) IF NOT EXISTS";
    public static final String INSERT_ENTITY_CLAUSES               = " (objectId, aclId, clock, entitySets, syncIds) VALUES( :"
            + ParamNames.OBJ_ID + ", :"
            + ParamNames.ACL_ID + ", toTimestamp(now()), :"
            + ParamNames.ENTITY_SETS + ", :"
            + ParamNames.SYNC_IDS + " ) IF objectId!=:"
            + ParamNames.OBJ_ID;

    // Read queries for datastore.
    public static final String GET_ALL_ENTITY_SETS                 = "select * from sparks."
            + DatastoreConstants.ENTITY_SETS_TABLE;
    public static final String GET_ENTITY_SET_BY_NAME              = "select * from sparks."
            + DatastoreConstants.ENTITY_SETS_TABLE + " where name = ?";
    public static final String GET_ALL_ENTITY_TYPES_QUERY          = "select * from sparks."
            + DatastoreConstants.ENTITY_TYPES_TABLE;
    public static final String GET_ALL_PROPERTY_TYPES_IN_NAMESPACE = "select * from sparks."
            + DatastoreConstants.PROPERTY_TYPES_TABLE + " where namespace=:"
            + ParamNames.NAMESPACE;
    public static final String GET_ALL_SCHEMAS_IN_NAMESPACE        = "select * from sparks."
            + DatastoreConstants.SCHEMAS_TABLE
            + " where namespace =:" + ParamNames.NAMESPACE + " AND aclId IN :"
            + ParamNames.ACL_IDS + " ALLOW filtering";
    public static final String GET_ALL_NAMESPACES                  = "select * from sparks."
            + DatastoreConstants.SCHEMAS_TABLE
            + " where aclId IN :"
            + ParamNames.ACL_IDS + " ALLOW filtering";
    public static final String ADD_ENTITY_TYPES_TO_SCHEMA          = "UPDATE sparks."
            + DatastoreConstants.SCHEMAS_TABLE
            + " SET entityTypeFqns = entityTypeFqns + :"
            + ParamNames.ENTITY_TYPES + " where aclId = :" + ParamNames.ACL_ID + " AND namespace = :"
            + ParamNames.NAMESPACE + " AND name = :"
            + ParamNames.NAME;
    public static final String REMOVE_ENTITY_TYPES_FROM_SCHEMA     = "UPDATE sparks."
            + DatastoreConstants.SCHEMAS_TABLE
            + " SET entityTypeFqns = entityTypeFqns - :"
            + ParamNames.ENTITY_TYPES + " where aclId = :" + ParamNames.ACL_ID + " AND namespace = :"
            + ParamNames.NAMESPACE + " AND name = :"
            + ParamNames.NAME;
    public static final String COUNT_ENTITY_SET                    = "select count(*) from sparks."
            + DatastoreConstants.ENTITY_SETS_TABLE + " where type = ? AND name = ?";
}