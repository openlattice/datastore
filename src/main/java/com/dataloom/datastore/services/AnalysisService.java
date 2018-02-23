package com.dataloom.datastore.services;

import com.openlattice.authorization.AccessCheck;
import com.openlattice.authorization.Permission;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.openlattice.analysis.requests.NeighborType;
import com.dataloom.authorization.*;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.EntityType;
import com.dataloom.graph.core.objects.NeighborTripletSet;
import com.google.common.collect.Sets;
import com.kryptnostic.datastore.services.EdmManager;
import com.openlattice.authorization.AclKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openlattice.analysis.requests.TopUtilizerDetails;
import com.dataloom.data.DataGraphManager;
import com.dataloom.data.DatasourceManager;
import com.openlattice.edm.type.PropertyType;
import com.google.common.collect.SetMultimap;

public class AnalysisService {

    private static final Logger logger = LoggerFactory.getLogger( AnalysisService.class );

    @Inject
    private DataGraphManager dgm;

    @Inject
    private DatasourceManager datasourceManager;

    @Inject
    private AuthorizationManager authorizations;

    @Inject
    private EdmManager edmManager;

    public Iterable<SetMultimap<Object, Object>> getTopUtilizers(
            UUID entitySetId,
            int numResults,
            List<TopUtilizerDetails> topUtilizerDetails,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        UUID syncId = datasourceManager.getCurrentSyncId( entitySetId );
        try {
            return dgm.getTopUtilizers( entitySetId, syncId, topUtilizerDetails, numResults, authorizedPropertyTypes );
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to get top utilizer data." );
            return null;
        }
    }

    public Iterable<NeighborType> getNeighborTypes( UUID entitySetId ) {
        UUID syncId = datasourceManager.getCurrentSyncId( entitySetId );

        NeighborTripletSet neighborEntitySets = dgm.getNeighborEntitySets( entitySetId, syncId );

        Set<UUID> entitySetIds = neighborEntitySets.stream().flatMap( triplet -> triplet.stream() )
                .collect( Collectors.toSet() );

        Set<UUID> authorizedEntitySetIds = authorizations
                .accessChecksForPrincipals( neighborEntitySets.stream().flatMap( triplet -> triplet.stream() )
                                .distinct().map( id -> new AccessCheck( new AclKey( id ),
                                        EnumSet.of( Permission.READ ) ) ).collect( Collectors.toSet() ),
                        Principals.getCurrentPrincipals() )
                .filter( authorization -> authorization.getPermissions().get( Permission.READ ) )
                .map( authorization -> authorization.getAclKey().get( 0 ) ).collect(
                        Collectors.toSet() );

        Map<UUID, EntitySet> entitySets = edmManager.getEntitySetsAsMap( authorizedEntitySetIds );

        Map<UUID, EntityType> entityTypes = edmManager
                .getEntityTypesAsMap( entitySets.values().stream()
                        .map( entitySet -> entitySet.getEntityTypeId() ).collect(
                                Collectors.toSet() ) );

        Set<NeighborType> neighborTypes = Sets.newHashSet();

        neighborEntitySets.forEach( triplet -> {
            boolean src = entitySetId.equals( triplet.get( 0 ) );
            UUID associationEntitySetId = triplet.get( 1 );
            UUID neighborEntitySetId = src ? triplet.get( 2 ) : triplet.get( 0 );
            if ( authorizedEntitySetIds.contains( associationEntitySetId ) && authorizedEntitySetIds
                    .contains( neighborEntitySetId ) ) {
                neighborTypes.add( new NeighborType(
                        entityTypes.get( entitySets.get( associationEntitySetId ).getEntityTypeId() ),
                        entityTypes.get( entitySets.get( neighborEntitySetId ).getEntityTypeId() ),
                        src ) );
            }
        } );

        return neighborTypes;

    }
}
