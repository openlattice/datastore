package com.dataloom.datastore.linking.services;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.dataloom.linking.Entity;
import com.dataloom.linking.components.Matcher;
import com.dataloom.linking.util.UnorderedPair;
import com.google.common.collect.Multimap;
import com.kryptnostic.datastore.services.EdmManager;

/**
 * The most basic version of Matcher. Use Jaro-Winkler to calculate scores.
 *
 */
public class SimpleMatcher implements Matcher {

    private Multimap<UUID, UUID>         linkingMap;
    private Set<Map<UUID, UUID>>         linkingProperties;

    private Map<Map<UUID, UUID>, Double> weights;

    private final EdmManager             dms;
    
    public SimpleMatcher(
            EdmManager dms ) {
        this.dms = dms;
    }

    @Override
    public double score( UnorderedPair<Entity> entityPair ) {
        Entity[] entities = entityPair.getAsArray();
        Entity elem0 = entities[0];
        Entity elem1 = entities[1];
        
        double score = 0;
        for ( Map<UUID, UUID> link : linkingProperties ) {
            UUID esId0 = elem0.getKey().getEntitySetId();
            UUID esId1 = elem1.getKey().getEntitySetId();
            
            if( link.containsKey( esId0 ) && link.containsKey( esId1 ) ){
                UUID ptId0 = link.get( esId0 );
                UUID ptId1 = link.get( esId1 );
                
                if( elem0.getProperties().containsKey( ptId0.toString() ) && elem1.getProperties().containsKey( ptId1.toString() ) ){
                    //TODO assuming singleton right now
                    //TODO very bad toString
                    String val0 = elem0.getProperties().get( ptId0.toString() ).toString();
                    String val1 = elem1.getProperties().get( ptId1.toString() ).toString();
                    
                    score += getScore( link, val0, val1 ) * getWeight( link );
                }
            }
        }
        return score;
    }

    @Override
    public void setLinking(
            Map<UUID, UUID> entitySetsWithSyncIds,
            Multimap<UUID, UUID> linkingMap,
            Set<Map<UUID, UUID>> linkingProperties ) {
        this.linkingMap = linkingMap;
        this.linkingProperties = linkingProperties;
        updateWeights();
    }

    private void updateWeights() {
        double totalWeight = 0;
        for ( Map<UUID, UUID> link : linkingProperties ) {
            double weight = getWeight( link );
            weights.put( link, weight );
            totalWeight += weight;
        }
        if ( totalWeight > 0 ) {
            normalizeWeights( totalWeight );
        } else {
            assignEqualWeights();
        }
    }

    private void normalizeWeights( double totalWeight ) {
        for ( Map<UUID, UUID> link : linkingProperties ) {
            double currentWeight = weights.get( link );
            weights.put( link, currentWeight / totalWeight );
        }
    }

    private void assignEqualWeights() {
        double weight = 1D / linkingProperties.size();
        for ( Map<UUID, UUID> link : linkingProperties ) {
            weights.put( link, weight );
        }
    }

    private Set<String> getPropertyNames( Map<UUID, UUID> link ){
        return dms.getPropertyTypes( new HashSet<>( link.values() ) ).stream()
        .map( pt -> pt.getType().getName() ).collect( Collectors.toSet() );
    }    

    // TODO lolz
    // Right now, 10 is something with heavy weight, 1 is smallest.
    private double getWeight( Map<UUID, UUID> link ) {
        Set<String> propertyNames = getPropertyNames( link );
        if ( propertyNames.contains( "year" ) || propertyNames.contains( "date" ) || propertyNames.contains( "dob" )
                || propertyNames.contains( "id" ) || propertyNames.contains( "ssn" ) ) {
            return 10;
        } else if ( propertyNames.contains( "name" ) || propertyNames.contains( "firstname" )
                || propertyNames.contains( "lastname" ) ) {
            return 7;
        }
        return 0;
    }
    
    /**
     * Use Jaro-Winkler for now.
     * @param link
     * @param val0
     * @param val1
     * @return
     */
    private double getScore( Map<UUID, UUID> link, String val0, String val1 ){
        return StringUtils.getJaroWinklerDistance( val0, val1 );
    }
}
