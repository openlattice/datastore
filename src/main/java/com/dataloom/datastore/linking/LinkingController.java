package com.dataloom.datastore.linking;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.data.EntityKey;
import com.dataloom.datasource.UUIDs.Syncs;
import com.dataloom.datastore.services.CassandraDataManager;
import com.dataloom.linking.Entity;
import com.dataloom.linking.LinkingApi;
import com.dataloom.linking.components.Blocker;
import com.dataloom.linking.components.Clusterer;
import com.dataloom.linking.components.Matcher;
import com.dataloom.linking.components.Merger;
import com.dataloom.linking.util.UnorderedPair;
import com.dataloom.streams.StreamUtil;
import com.google.common.base.Preconditions;
import com.kryptnostic.datastore.services.EdmManager;

public class LinkingController implements LinkingApi, AuthorizingComponent {

    @Inject
    private AuthorizationManager authz;

    @Inject
    private EdmManager dms;
    
    @Inject
    private CassandraDataManager dataManager;
    
    @Override
    public UUID linkEntitySets( Set<Map<UUID, UUID>> linkingProperties ) {
        Set<UUID> linkingES = validateAndGetLinkingEntitySets( linkingProperties );
        
        Blocker blocker = new SimpleElasticSearchBlocker( linkingES, linkingProperties );
        Iterable<UnorderedPair<Entity>> pairs = blocker.block();
        Set<UnorderedPair<Entity>> treatedPairs = new HashSet<>();

        Matcher matcher = new SimpleMatcher( linkingES, linkingProperties );
        Map<UnorderedPair<Entity>, Double> scores = StreamUtil.stream( pairs ).map( pair -> {
            if( treatedPairs.contains( pair ) ){
                return null;
            }
            return pair;
        }).filter( Objects::nonNull )
        .collect( Collectors.toMap( pair -> pair, matcher::score ) );
        
        //Use WEKA for clustering? Reference: http://weka.sourceforge.net/doc.dev/weka/clusterers/HierarchicalClusterer.html
        //Basic prototype: constructor should take set of vertices and a distance function between vertices.
        Clusterer clusterer = new SimpleHierarchicalClusterer( scores.keySet(), (v,w) -> scores.get( new UnorderedPair(v,w) ) );
        Merger merger = new SimpleMerger( linkingES, linkingProperties );
        
        Map<Map<UUID, UUID>, UUID> linkedPropertyTypes = generateLinkedPropertyTypes( linkingProperties );
        UUID entitySetId = generateLinkedEntitySet( linkingES, linkedPropertyTypes );
        UUID syncId = Syncs.LINK.getSyncId();
        
        StreamUtil.stream( clusterer.cluster() )
        .map( merger::merge )
        //TODO add a createEntityData method in CassandraDataManager that does not need to take datatype; by default uses jackson byte serialization.
        //TODO Perhaps add an extra step in the stream to batch process linkedEntities.
        .forEach( linkedEntity -> dataManager.createEntityData( entitySetId, syncId, linkedEntity ) );
        
        return entitySetId;        
    }

    @Override
    public UUID linkEntities( UUID syncId, UUID entitySetId, UUID entityId, Set<EntityKey> entities ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void setLinkedEntities( UUID syncId, UUID entitySetId, UUID entityId, Set<EntityKey> entities ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void deleteLinkedEntities( UUID syncId, UUID entitySetId, UUID entityId ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void addLinkedEntities( UUID syncId, UUID entitySetId, UUID entityId, UUID linkedEntityId ) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Void removeLinkedEntity( UUID syncId, UUID entitySetId, UUID entityId, UUID linkedEntityId ) {
        // TODO Auto-generated method stub
        return null;
    }

    private Set<UUID> validateAndGetLinkingEntitySets( Set<Map<UUID, UUID>> linkingProperties ) {
        Set<UUID> linkingSets = new HashSet<>();

        linkingProperties.stream().map( m -> m.entrySet() )
        .forEach( set -> {
            Preconditions.checkArgument( set.size() > 1, "Map cannot have length 1, which does not match any properties." );
            for( Entry<UUID, UUID> entry : set ){
                List<UUID> aclKey = Arrays.asList( entry.getKey(), entry.getValue() );
                ensureLinkAccess( aclKey );
                //Add entity set id to linking Sets if nothing wrong happens so far
                linkingSets.add( aclKey.get( 0 ) );
            }
        });
        //Sanity check
        linkingSets.stream().forEach( entitySetId -> ensureLinkAccess( Arrays.asList( entitySetId )) );
        
        return linkingSets;
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authz;
    }

}
