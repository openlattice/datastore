package com.dataloom.datastore.analysis.controllers;

import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dataloom.analysis.AnalysisApi;
import com.dataloom.analysis.requests.TopUtilizerDetails;
import com.dataloom.analysis.requests.TopUtilizersHistogramRequest;
import com.dataloom.analysis.requests.TopUtilizersHistogramResult;
import com.dataloom.authorization.EdmAuthorizationHelper;
import com.dataloom.authorization.Permission;
import com.dataloom.data.EntitySetData;
import com.dataloom.data.requests.FileType;
import com.dataloom.data.requests.NeighborEntityDetails;
import com.dataloom.datastore.constants.CustomMediaType;
import com.dataloom.datastore.services.AnalysisService;
import com.dataloom.datastore.services.SearchService;
import com.dataloom.edm.type.PropertyType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.kryptnostic.datastore.services.EdmService;

@RestController
@RequestMapping( AnalysisApi.CONTROLLER )
public class AnalysisController implements AnalysisApi {

    @Inject
    private AnalysisService        analysisService;

    @Inject
    private EdmService             edm;

    @Inject
    private EdmAuthorizationHelper authorizationsHelper;

    @Inject
    private SearchService          searchService;

    private static void setDownloadContentType( HttpServletResponse response, FileType fileType ) {
        if ( fileType == FileType.csv ) {
            response.setContentType( CustomMediaType.TEXT_CSV_VALUE );
        } else {
            response.setContentType( MediaType.APPLICATION_JSON_VALUE );
        }
    }

    @RequestMapping(
        path = { ENTITY_SET_ID_PATH + NUM_RESULTS_PATH },
        method = RequestMethod.POST,
        produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    public EntitySetData<Object> getTopUtilizers(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( NUM_RESULTS ) int numResults,
            @RequestBody List<TopUtilizerDetails> topUtilizerDetails,
            @RequestParam(
                value = FILE_TYPE,
                required = false ) FileType fileType,
            HttpServletResponse response ) {
        FileType downloadType = ( fileType == null ) ? FileType.json : fileType;
        // setContentDisposition( response, entitySetId.toString(), downloadType );
        setDownloadContentType( response, downloadType );
        return getTopUtilizers( entitySetId, numResults, topUtilizerDetails, downloadType );
    }

    @Override
    public EntitySetData<Object> getTopUtilizers(
            UUID entitySetId,
            int numResults,
            List<TopUtilizerDetails> topUtilizerDetails,
            FileType fileType ) {
        if ( topUtilizerDetails.size() == 0 ) return null;
        Set<UUID> authorizedProperties = authorizationsHelper.getAuthorizedPropertiesOnEntitySet( entitySetId,
                EnumSet.of( Permission.READ ) );
        if ( authorizedProperties.size() == 0 ) return null;
        Map<UUID, PropertyType> authorizedPropertyTypes = edm.getPropertyTypesAsMap( authorizedProperties );
        Iterable<SetMultimap<Object, Object>> utilizers = analysisService.getTopUtilizers( entitySetId,
                numResults,
                topUtilizerDetails,
                authorizedPropertyTypes );
        LinkedHashSet<String> columnTitles = authorizedPropertyTypes.values().stream().map( pt -> pt.getType() )
                .map( fqn -> fqn.toString() )
                .collect( Collectors.toCollection( () -> new LinkedHashSet<String>() ) );
        columnTitles.add( "count" );
        columnTitles.add( "id" );
        return new EntitySetData<Object>( columnTitles, utilizers );
    }

    @RequestMapping(
        path = { ENTITY_SET_ID_PATH + NUM_RESULTS_PATH + HISTOGRAM_PATH },
        method = RequestMethod.POST,
        produces = { MediaType.APPLICATION_JSON_VALUE } )
    @Override
    public TopUtilizersHistogramResult getTopUtilizersHistogram(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( NUM_RESULTS ) int numResults,
            @RequestBody TopUtilizersHistogramRequest histogramDetails ) {
        EntitySetData<Object> topUtilizers = getTopUtilizers( entitySetId,
                numResults,
                histogramDetails.getTopUtilizerDetails(),
                FileType.json );
        List<SetMultimap<Object, Object>> utilizers = Lists.newArrayList( topUtilizers.getEntities() );
        Map<UUID, List<NeighborEntityDetails>> neighborDetails = Maps.newHashMap();
        utilizers.forEach( entity -> {
            UUID entityId = UUID.fromString( (String) entity.get( "id" ).iterator().next() );
            neighborDetails.put( entityId, searchService.executeEntityNeighborSearch( entityId ) );
        } );
        return analysisService.getTopUtilizersHistogram(
                edm.getEntityTypeByEntitySetId( entitySetId ),
                utilizers,
                histogramDetails,
                neighborDetails );
    }

}
