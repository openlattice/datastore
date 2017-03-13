package com.dataloom.datastore.scripts;

import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;

public class EmptyPermissionRemover {
    private String                  keyspace;
    private Session                 session;
    private final PreparedStatement deleteRowQuery;

    public EmptyPermissionRemover(
            String keyspace,
            Session session ) {
        this.keyspace = keyspace;
        this.session = session;
        this.deleteRowQuery = session
                .prepare( QueryBuilder.delete().from( keyspace, Table.PERMISSIONS.getName() ).where(
                        QueryBuilder.eq( CommonColumns.ACL_KEYS.cql(), CommonColumns.ACL_KEYS.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PRINCIPAL_TYPE.cql(),
                                CommonColumns.PRINCIPAL_TYPE.bindMarker() ) )
                        .and( QueryBuilder.eq( CommonColumns.PRINCIPAL_ID.cql(),
                                CommonColumns.PRINCIPAL_ID.bindMarker() ) ) );

        // Trigger script to remove empty permissions in permissions table
        removeEmptyPermissions();
    }

    public void removeEmptyPermissions() {
        ResultSet rs = session.execute( QueryBuilder.select().all().from( keyspace, Table.PERMISSIONS.getName() ) );

        for ( Row row : rs ) {
            EnumSet<Permission> permissions = AuthorizationUtils.permissions( row );
            if ( permissions.isEmpty() ) {
                List<UUID> aclKeys = AuthorizationUtils.aclKey( row );
                Principal principal = AuthorizationUtils.getPrincipalFromRow( row );
                session.execute( deleteRowQuery.bind()
                        .setList( CommonColumns.ACL_KEYS.cql(),
                                aclKeys,
                                UUID.class )
                        .set( CommonColumns.PRINCIPAL_TYPE.cql(), principal.getType(), PrincipalType.class )
                        .setString( CommonColumns.PRINCIPAL_ID.cql(), principal.getId() ) );
            }
        }
    }
}
