/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.datastore.services;

import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.NotImplementedException;

public class DatasourceManager {
    public UUID createDatasource( UUID aclId, String name, String description, UUID syncId ) {
        throw new NotImplementedException( "MTR WAS HERE." );
    }

    public UUID initializeSync( UUID datasourceId ) {
        throw new NotImplementedException( "MTR WAS HERE." );
    }

    public void finalizeSync( UUID syncId ) {
        throw new NotImplementedException( "MTR WAS HERE." );
    }

    public Set<UUID> getOutstandingSyncs( UUID datasourceId ) {
        throw new NotImplementedException( "MTR WAS HERE." );
    }
}
