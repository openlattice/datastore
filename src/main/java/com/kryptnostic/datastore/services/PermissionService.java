package com.kryptnostic.datastore.services;

import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;

public class PermissionService implements PermissionApi{

	/**
	 * @Inject
	 * private UUID activeUUID;
	 */
	
	@Override
	public void addPermission(Set<FullQualifiedName> types, Set<String> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addPermission(FullQualifiedName type, Set<String> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removePermission(Set<FullQualifiedName> types, Set<String> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removePermission(FullQualifiedName type, Set<String> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPermission(Set<FullQualifiedName> types, Set<String> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPermission(FullQualifiedName type, Set<String> permissions) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void checkPermission(FullQualifiedName type, String action) {
		// TODO Auto-generated method stub
		
	}

}
