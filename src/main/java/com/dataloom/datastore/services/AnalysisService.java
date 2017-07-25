package com.dataloom.datastore.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Maps;

import com.clearspring.analytics.util.Lists;
import com.dataloom.analysis.requests.TopUtilizerDetails;
import com.dataloom.analysis.requests.TopUtilizersHistogramRequest;
import com.dataloom.analysis.requests.TopUtilizersHistogramResult;
import com.dataloom.data.DataGraphManager;
import com.dataloom.data.DatasourceManager;
import com.dataloom.data.requests.NeighborEntityDetails;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.google.common.base.Optional;
import com.google.common.collect.SetMultimap;
import com.kryptnostic.datastore.services.EdmManager;

import jersey.repackaged.com.google.common.collect.Sets;

public class AnalysisService {

    private static final Logger logger = LoggerFactory.getLogger( AnalysisService.class );

    @Inject
    private DataGraphManager    dgm;

    @Inject
    private DatasourceManager   datasourceManager;

    @Inject
    private EdmManager          dms;

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

    private List<String> getFieldPropertyValues(
            SetMultimap<Object, Object> utilizer,
            UUID utilizerEntityTypeId,
            UUID entityTypeId,
            PropertyType propertyType,
            List<NeighborEntityDetails> neighbors ) {
        List<String> values = Lists.newArrayList();
        if ( utilizerEntityTypeId.equals( entityTypeId ) ) {
            utilizer.get( propertyType.getType() ).forEach( value -> {
                values.add( value.toString() );
            } );
        }
        neighbors.forEach( neighborDetails -> {
            if ( neighborDetails.getNeighborEntitySet().isPresent()
                    && neighborDetails.getNeighborEntitySet().get().getEntityTypeId().equals( entityTypeId )
                    && neighborDetails.getNeighborDetails().isPresent() ) {
                neighborDetails.getNeighborDetails().get().get( propertyType.getType() )
                        .forEach( value -> values.add( value.toString() ) );
            }
        } );
        return values;
    }

    public TopUtilizersHistogramResult getTopUtilizersHistogram(
            EntityType utilizerEntityType,
            List<SetMultimap<Object, Object>> topUtilizers,
            TopUtilizersHistogramRequest histogramDetails,
            Map<UUID, List<NeighborEntityDetails>> neighborDetails ) {
        List<Map<String, String>> resultList = Lists.newArrayList();
        Map<String, Map<String, Integer>> counts = Maps.newHashMap();
        Set<String> fields = Sets.newHashSet();

        boolean isSimple = !histogramDetails.getDrillDownEntityTypeId().isPresent()
                || !histogramDetails.getDrillDownPropertyTypeId().isPresent();
        if ( isSimple ) fields.add( "count" );

        PropertyType primaryPropertyType = dms.getPropertyType( histogramDetails.getPrimaryPropertyTypeId() );
        Optional<PropertyType> drillDownPropertyType = ( isSimple ) ? Optional.absent()
                : Optional.of( dms.getPropertyType( histogramDetails.getDrillDownPropertyTypeId().get() ) );
        topUtilizers.forEach( utilizer -> {
            UUID entityId = UUID.fromString( (String) utilizer.get( "id" ).iterator().next() );
            List<String> primaryPropertyValues = getFieldPropertyValues( utilizer,
                    utilizerEntityType.getId(),
                    histogramDetails.getPrimaryEntityTypeId(),
                    primaryPropertyType,
                    neighborDetails.get( entityId ) );
            primaryPropertyValues.forEach( primaryPropertyValue -> {
                if ( !counts.containsKey( primaryPropertyValue ) )
                    counts.put( primaryPropertyValue, new HashMap<String, Integer>() );
                List<String> fieldNames = ( isSimple ) ? Lists.newArrayList( fields )
                        : getFieldPropertyValues( utilizer,
                                utilizerEntityType.getId(),
                                histogramDetails.getDrillDownEntityTypeId().get(),
                                drillDownPropertyType.get(),
                                neighborDetails.get( entityId ) );
                fieldNames.forEach( fieldName -> {
                    fields.add( fieldName );
                    int count = counts.get( primaryPropertyValue ).containsKey( fieldName )
                            ? counts.get( primaryPropertyValue ).get( fieldName ) + 1 : 1;
                    counts.get( primaryPropertyValue ).put( fieldName, count );
                } );
            } );

        } );

        counts.entrySet().stream().forEach( entry -> {
            Map<String, String> histogramValues = Maps.newHashMap();
            histogramValues.put( "name", entry.getKey() );
            entry.getValue().entrySet().stream().forEach( fieldCounts -> {
                histogramValues.put( fieldCounts.getKey(), fieldCounts.getValue().toString() );
            } );
            resultList.add( histogramValues );
        } );

        return new TopUtilizersHistogramResult( resultList, fields );
    }

}
