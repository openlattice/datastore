package com.dataloom.datastore.scripts;

import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.directory.UserDirectoryService;
import com.dataloom.hazelcast.HazelcastMap;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Iterables;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.services.EdmManager;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.edm.EntitySet;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

public class EntitySetContactsPopulator implements Serializable {
    private static final long serialVersionUID = -6252192257512539448L;
    private String               keyspace;
    private Session              session;
    private EdmManager           dms;
    private UserDirectoryService uds;

    private final IMap<UUID, EntitySet> entitySets;

    public EntitySetContactsPopulator(
            String keyspace,
            Session session,
            EdmManager dms,
            UserDirectoryService uds,
            HazelcastInstance hazelcastInstance ) {
        this.keyspace = keyspace;
        this.session = session;
        this.dms = dms;
        this.uds = uds;
        this.entitySets = hazelcastInstance.getMap( HazelcastMap.ENTITY_SETS.name() );
    }

    public void run() {
        //        StreamUtil.stream( dms.getEntitySets() )
        //                .filter( es -> es.getContacts() == null || es.getContacts().isEmpty() )
        //                .forEach( es -> {
        //                    Set<String> contacts = StreamUtil.stream( getOwnerForEntitySet( Arrays.asList( es.getId() ) ) )
        //                            .map( principal -> getUserAsString( principal ) )
        //                            .collect( Collectors.toSet() );
        //
        //                    if( contacts.isEmpty() ){
        //                        contacts = ImmutableSet.of( "No contacts found" );
        //                    }
        //
        //                    entitySets.executeOnKey( es.getId(), new UpdateEntitySetContactsProcessor( contacts ) );
        //        } );
    }

    private Iterable<Principal> getOwnerForEntitySet( List<UUID> entitySetId ) {
        ResultSet rs = session.execute( getOwnerQuery().bind().setList( CommonColumns.ACL_KEYS.cql(),
                entitySetId,
                UUID.class ) );
        return Iterables.transform( rs, AuthorizationUtils::getPrincipalFromRow );
    }

    private PreparedStatement getOwnerQuery() {
        return session.prepare( QueryBuilder
                .select()
                .from( keyspace, Table.PERMISSIONS.getName() ).allowFiltering()
                .where( QueryBuilder.eq( CommonColumns.ACL_KEYS.cql(),
                        CommonColumns.ACL_KEYS.bindMarker() ) )
                .and( QueryBuilder.eq( CommonColumns.PRINCIPAL_TYPE.cql(), PrincipalType.USER ) )
                .and( QueryBuilder.contains( CommonColumns.PERMISSIONS.cql(), Permission.OWNER ) ) );
    }

    private String getUserAsString( Principal principal ) {
        return uds.getUser( principal.getId() ).getUsername();
    }

}
