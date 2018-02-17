package com.dataloom.datastore.scripts;

import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principal;
import com.google.common.collect.ImmutableList;
import com.openlattice.postgres.*;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

public class EmptyPermissionRemover {
    private       HikariDataSource hds;
    private final String           allPermissionsSql;
    private final String           deleteRowSql;

    public EmptyPermissionRemover( HikariDataSource hds ) {
        this.hds = hds;

        // Tables
        String PERMISSIONS = PostgresTable.PERMISSIONS.getName();

        // Columns
        String ACL_KEY = PostgresColumn.ACL_KEY.getName();
        String PRINCIPAL_TYPE = PostgresColumn.PRINCIPAL_TYPE.getName();
        String PRINCIPAL_ID = PostgresColumn.PRINCIPAL_ID.getName();

        this.allPermissionsSql = PostgresQuery.selectFrom( PERMISSIONS ).concat( PostgresQuery.END );

        this.deleteRowSql = PostgresQuery.deleteFrom( PERMISSIONS )
                .concat( PostgresQuery.whereEq( ImmutableList.of( ACL_KEY, PRINCIPAL_TYPE, PRINCIPAL_ID ), true ) );

    }

    public void run() {
        try ( Connection connection = hds.getConnection() ) {
            ResultSet rs = connection.prepareStatement( allPermissionsSql ).executeQuery();
            while ( rs.next() ) {
                EnumSet<Permission> permissions = ResultSetAdapters.permissions( rs );
                if ( permissions == null || permissions.isEmpty() ) {
                    List<UUID> aclKey = ResultSetAdapters.aclKey( rs );
                    Principal principal = ResultSetAdapters.principal( rs );
                    PreparedStatement ps = connection.prepareStatement( deleteRowSql );
                    ps.setArray( 1, PostgresArrays.createUuidArray( connection, aclKey.stream() ) );
                    ps.setString( 2, principal.getType().name() );
                    ps.setString( 3, principal.getId() );
                    ps.execute();
                }
            }
            connection.close();
        } catch ( SQLException e ) {}
    }
}
