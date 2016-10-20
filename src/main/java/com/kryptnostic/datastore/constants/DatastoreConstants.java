package com.kryptnostic.datastore.constants;

public class DatastoreConstants {

    private DatastoreConstants() {}

    // Parameter that decides file type of response when added
    // Example: /data/entitydata/{entity_type_namespace}/{entity_type_name}?FILE_TYPE=json returns json
    public static final String FILE_TYPE = "fileType";
}
