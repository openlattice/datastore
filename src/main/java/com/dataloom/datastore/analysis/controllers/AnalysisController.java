package com.dataloom.datastore.analysis.controllers;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.dataloom.analysis.AnalysisApi;
import com.dataloom.analysis.requests.TopUtilizerDetails;
import com.dataloom.authorization.EdmAuthorizationHelper;
import com.dataloom.authorization.Permission;
import com.dataloom.data.EntitySetData;
import com.dataloom.datastore.services.AnalysisService;
import com.dataloom.edm.type.PropertyType;
import com.google.common.collect.Lists;
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

    @RequestMapping(
        path = { ENTITY_SET_ID_PATH + NUM_RESULTS_PATH },
        method = RequestMethod.POST )
    @Override
    public EntitySetData getTopUtilizers(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( NUM_RESULTS ) int numResults,
            @RequestBody List<TopUtilizerDetails> topUtilizerDetails ) {
        Set<UUID> authorizedProperties = authorizationsHelper.getAuthorizedPropertiesOnEntitySet( entitySetId,
                EnumSet.of( Permission.READ ) );
        if ( authorizedProperties.size() == 0 ) return null;
        Map<UUID, PropertyType> authorizedPropertyTypes = edm.getPropertyTypesAsMap( authorizedProperties );
        return analysisService.getTopUtilizers( entitySetId, numResults, topUtilizerDetails, authorizedPropertyTypes );
    }

}
