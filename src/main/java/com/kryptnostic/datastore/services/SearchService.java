package com.kryptnostic.datastore.services;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;
import com.dataloom.authorization.Ace;
import com.dataloom.authorization.AclKeyPathFragment;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.SecurableObjectType;
import com.google.common.base.Optional;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.durableexecutor.DurableExecutorService;
import com.kryptnostic.conductor.rpc.ConductorCall;
import com.kryptnostic.conductor.rpc.Lambdas;

public class SearchService {
	
	private static final Logger logger = LoggerFactory.getLogger( SearchService.class );
	
	private final DurableExecutorService executor;
	
	public SearchService( HazelcastInstance hazelcast ) {
		this.executor = hazelcast.getDurableExecutorService( "default" );
	}
	
	public List<Map<String, Object>> executeEntitySetKeywordSearchQuery( String query, Optional<UUID> optionalEntityType, Optional<Set<UUID>> optionalPropertyTypes ) {
		try {
			List<Map<String, Object>> queryResults = executor.submit( ConductorCall
					.wrap( Lambdas.executeElasticsearchMetadataQuery( query, optionalEntityType, optionalPropertyTypes ) ) )
			.get();
			return queryResults;
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public void updateEntitySetPermissions( List<AclKeyPathFragment> aclKeys, Principal principal, Set<Permission> permissions ) {
		aclKeys.forEach( aclKey -> {
			if ( aclKey.getType() == SecurableObjectType.EntitySet ) {
				executor.submit( ConductorCall
						.wrap( Lambdas.updateEntitySetPermissions( aclKey.getId(), principal, permissions) ) );
			}
		} );
	}

}
