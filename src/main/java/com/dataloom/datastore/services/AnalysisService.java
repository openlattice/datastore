package com.dataloom.datastore.services;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.edm.type.PropertyType;
import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.dataloom.data.storage.CassandraEntityDatastore;
import com.dataloom.data.DatasourceManager;

public class AnalysisService {

    private static final Logger        logger = LoggerFactory.getLogger( AnalysisService.class );

    @Inject
    private DatastoreConductorSparkApi sparkApi;

    @Inject
    private CassandraEntityDatastore cdm;
    
    @Inject
    private DatasourceManager                         datasourceManager;

    public List<SetMultimap<UUID, Object>> getTopUtilizers(
            UUID entitySetId,
            Set<UUID> propertyTypeIds,
            int maxHits,
            Map<UUID, PropertyType> propertyTypes ) {
        UUID syncId = datasourceManager.getCurrentSyncId( entitySetId );
        UUID requestId = sparkApi.getTopUtilizers( entitySetId, syncId, propertyTypeIds, propertyTypes );
        List<SetMultimap<UUID, Object>> results = Lists.newArrayList();
        Iterator<byte[]> byteResults = cdm.readNumRPCRows( requestId, maxHits ).iterator();
        TypeReference<SetMultimap<UUID, Object>> resultType = new TypeReference<SetMultimap<UUID, Object>>() {};
        while ( byteResults.hasNext() ) {
            try {
                SetMultimap<UUID, Object> row = ObjectMappers.getSmileMapper().readValue( byteResults.next(),
                        resultType );
                results.add( row );
            } catch ( IOException e ) {
                logger.debug( "unable to read row from binary rpc data" );
            }
        }

        return results;
    }

}
