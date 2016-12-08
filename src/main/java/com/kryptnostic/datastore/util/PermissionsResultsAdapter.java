package com.kryptnostic.datastore.util;

import com.dataloom.authorization.requests.PermissionsInfo;
import com.dataloom.authorization.requests.PropertyTypeInEntitySetAclRequestWithRequestingUser;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.services.UserDirectoryService;

public class PermissionsResultsAdapter {

    private static final String  ID_TO_NAME_CACHE = "id_to_name_cache";
    private IMap<String, String> idToNameCache;

    private UserDirectoryService uds;

    public PermissionsResultsAdapter( HazelcastInstance hazelcastInstance, UserDirectoryService uds ) {
        this.uds = uds;
        idToNameCache = hazelcastInstance.getMap( ID_TO_NAME_CACHE );
    }

    public PropertyTypeInEntitySetAclRequestWithRequestingUser mapUserIdToName(
            PropertyTypeInEntitySetAclRequestWithRequestingUser req ) {
        req.setRequestingUser( getUsernameFromId( req.getRequestingUser() ) );

        return req;
    }

    public PermissionsInfo mapUserIdToName( PermissionsInfo req ) {
        return req;
    }

    private String getUsernameFromId( String userId ) {
        if ( idToNameCache.containsKey( userId ) ) {
            return idToNameCache.get( userId );
        } else {
            String username = uds.getUser( userId ).getUsername();
            idToNameCache.put( userId, username );
            return username;
        }
    }

}
