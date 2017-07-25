package com.dataloom.datastore.services;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.analysis.requests.TopUtilizerDetails;
import com.dataloom.data.DataGraphManager;
import com.dataloom.data.DatasourceManager;
import com.dataloom.edm.type.PropertyType;
import com.google.common.collect.SetMultimap;

public class AnalysisService {

    private static final Logger logger = LoggerFactory.getLogger( AnalysisService.class );

    @Inject
    private DataGraphManager    dgm;

    @Inject
    private DatasourceManager   datasourceManager;

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
}
