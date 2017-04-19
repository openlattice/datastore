package com.dataloom.datastore.services;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.analysis.requests.TopUtilizerDetails;
import com.dataloom.data.DataGraphService;
import com.dataloom.data.DatasourceManager;
import com.dataloom.data.EntitySetData;
import com.dataloom.edm.type.PropertyType;

public class AnalysisService {

    private static final Logger logger = LoggerFactory.getLogger( AnalysisService.class );

    @Inject
    private DataGraphService    dgs;

    @Inject
    private DatasourceManager   datasourceManager;

    public EntitySetData getTopUtilizers(
            UUID entitySetId,
            int numResults,
            List<TopUtilizerDetails> topUtilizerDetails,
            Map<UUID, PropertyType> authorizedPropertyTypes ) {
        UUID syncId = datasourceManager.getCurrentSyncId( entitySetId );
        try {
            return dgs.getTopUtilizers( entitySetId, syncId, topUtilizerDetails, numResults, authorizedPropertyTypes );
        } catch ( InterruptedException | ExecutionException e ) {
            logger.debug( "Unable to get top utilizer data." );
            return null;
        }
    }

}
