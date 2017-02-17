package com.dataloom.datastore.linking.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.lang3.StringUtils;

import com.dataloom.linking.Entity;
import com.dataloom.linking.components.Matcher;
import com.dataloom.linking.util.UnorderedPair;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.kryptnostic.datastore.services.EdmManager;

/**
 * The most basic version of Matcher. Use Jaro-Winkler to calculate scores.
 *
 */
public class SimpleMatcher implements Matcher {

    private SetMultimap<UUID, UUID> linkIndexedByPropertyTypes;
    private Set<UUID>               linkingProperties;

    private Map<UUID, Double>       weights;

    private static DoubleMetaphone  doubleMetaphone = new DoubleMetaphone();

    private final EdmManager        dms;

    public SimpleMatcher(
            EdmManager dms ) {
        this.dms = dms;
    }

    @Override
    public double dist( UnorderedPair<Entity> entityPair ) {
        List<Entity> entities = entityPair.getAsList();
        Entity elem0 = entities.get( 0 );
        Entity elem1 = entities.get( 1 );

        UUID esId0 = elem0.getKey().getEntitySetId();
        UUID esId1 = elem1.getKey().getEntitySetId();

        double dist = 0;
        for ( UUID propertyTypeId : linkingProperties ) {

            String pidAsString = propertyTypeId.toString();

            if ( linkIndexedByPropertyTypes.get( propertyTypeId ).containsAll( ImmutableSet.of( esId0, esId1 ) ) ) {
                // This property type is linked
                Object val0 = elem0.getProperties().get( pidAsString );
                Object val1 = elem1.getProperties().get( pidAsString );
                if ( val0 != null && val1 != null ) {
                    // Both values are non-null; score can be computed.
                    dist += getDistance( propertyTypeId, val0, val1 ) * weights.get( propertyTypeId );
                }
            }
        }
        return dist;
    }

    @Override
    public void setLinking(
            Map<UUID, UUID> entitySetsWithSyncIds,
            SetMultimap<UUID, UUID> linkIndexedByPropertyTypes,
            SetMultimap<UUID, UUID> linkIndexedByEntitySets ) {
        this.linkIndexedByPropertyTypes = linkIndexedByPropertyTypes;
        this.linkingProperties = linkIndexedByPropertyTypes.keySet();
        updateWeights();
    }

    private void updateWeights() {
        weights = new HashMap<>();
        double totalWeight = 0;

        for ( UUID propertyTypeId : linkingProperties ) {
            double weight = getDefaultUnnormalizedWeight( propertyTypeId );
            weights.put( propertyTypeId, weight );
            totalWeight += weight;
        }
        if ( totalWeight > 0 ) {
            normalizeWeights( totalWeight );
        } else {
            assignEqualWeights();
        }
    }

    private void normalizeWeights( double totalWeight ) {
        for ( UUID propertyTypeId : linkingProperties ) {
            double currentWeight = weights.get( propertyTypeId );
            weights.put( propertyTypeId, currentWeight / totalWeight );
        }
    }

    private void assignEqualWeights() {
        double weight = 1D / linkingProperties.size();
        for ( UUID propertyTypeId : linkingProperties ) {
            weights.put( propertyTypeId, weight );
        }
    }

    // TODO lolz
    // Right now, 10 is something with heavy weight, 1 is smallest.
    private double getDefaultUnnormalizedWeight( UUID propertyTypeId ) {
        String propertyName = getPropertyName( propertyTypeId );
        if ( propertyName.contains( "year" ) || propertyName.contains( "date" ) || propertyName.contains( "dob" )
                || propertyName.contains( "id" ) || propertyName.contains( "ssn" ) ) {
            return 10;
        } else if ( propertyName.contains( "name" ) || propertyName.contains( "firstname" )
                || propertyName.contains( "lastname" ) ) {
            return 7;
        }
        return 1;
    }

    private double getDistance( UUID propertyTypeId, Object val0, Object val1 ) {
        Set<String> set0;
        Set<String> set1;
        // TODO update the terrible toString's
        if ( val0 instanceof Set<?> ) {
            set0 = ( (Set<?>) val0 ).stream().map( obj -> obj.toString() ).collect( Collectors.toSet() );
        } else {
            set0 = ImmutableSet.of( val0.toString() );
        }

        if ( val1 instanceof Set<?> ) {
            set1 = ( (Set<?>) val1 ).stream().map( obj -> obj.toString() ).collect( Collectors.toSet() );
        } else {
            set1 = ImmutableSet.of( val1.toString() );
        }

        return getDistance( propertyTypeId, set0, set1 );
    }

    private double getDistance( UUID propertyTypeId, Set<String> val0, Set<String> val1 ) {
        double minDist = Double.POSITIVE_INFINITY;

        for ( String s0 : val0 ) {
            for ( String s1 : val1 ) {
                double currentDist = getDistance( propertyTypeId, s0, s1 );
                if ( currentDist < minDist ) {
                    minDist = currentDist;
                }
            }
        }
        return minDist;
    }

    /**
     * Use Jaro-Winkler for now.
     * 
     * @param link
     * @param val0
     * @param val1
     * @return
     */
    private double getDistance( UUID propertyTypeId, String val0, String val1 ) {
        switch ( getPropertyName( propertyTypeId ) ) {
            case "name":
            case "firstname":
            case "lastname":
                return 1 - StringUtils.getJaroWinklerDistance( doubleMetaphone.encode( val0 ),
                        doubleMetaphone.encode( val1 ) );
            default:
                return 1 - StringUtils.getJaroWinklerDistance( val0, val1 );
        }
    }

    private String getPropertyName( UUID propertyTypeId ) {
        return dms.getPropertyType( propertyTypeId ).getType().getName();
    }
}
