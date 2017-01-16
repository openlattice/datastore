package com.dataloom.datastore.data.controllers;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.EdmAuthorizationHelper;
import com.dataloom.authorization.ForbiddenException;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principals;
import com.dataloom.data.DataApi;
import com.dataloom.data.requests.EntitySetSelection;
import com.dataloom.datastore.constants.CustomMediaType;
import com.dataloom.datastore.services.CassandraDataManager;
import com.dataloom.edm.internal.PropertyType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.kryptnostic.datastore.services.EdmService;

@RestController
public class DataController implements DataApi {

    @Inject
    private EdmService             dms;

    @Inject
    private CassandraDataManager   cdm;

    @Inject
    private AuthorizationManager   authz;

    @Inject
    private EdmAuthorizationHelper authzHelper;

    @RequestMapping(
        path = { "/" + CONTROLLER + "/" + ENTITY_DATA + "/" + SET_ID_PATH },
        method = RequestMethod.GET,
        produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    public Iterable<SetMultimap<FullQualifiedName, Object>> getEntitySetData(
            @PathVariable( SET_ID ) UUID entitySetId,
            @RequestParam(
                value = FILE_TYPE,
                required = false ) FileType fileType,
            HttpServletResponse response ) {
        setContentDisposition( response, entitySetId.toString(), fileType );
        setDownloadContentType( response, fileType );
        return getEntitySetData( entitySetId, fileType );
    }

    @Override
    public Iterable<SetMultimap<FullQualifiedName, Object>> getEntitySetData( UUID entitySetId, FileType fileType ) {
        return getEntitySetData( entitySetId, Optional.absent(), Optional.absent() );
    }

    @RequestMapping(
        path = { "/" + CONTROLLER + "/" + HISTORICAL + "/" + ENTITY_DATA + "/" + SET_ID_PATH },
        method = RequestMethod.GET,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    public Iterable<SetMultimap<FullQualifiedName, Object>> getEntitySetData(
            @PathVariable( SET_ID ) UUID entitySetId,
            @RequestBody EntitySetSelection req,
            @RequestParam(
                value = FILE_TYPE,
                required = false ) FileType fileType,
            HttpServletResponse response ) {
        setContentDisposition( response, entitySetId.toString(), fileType );
        setDownloadContentType( response, fileType );
        return getEntitySetData( entitySetId, req, fileType );
    }

    @Override
    public Iterable<SetMultimap<FullQualifiedName, Object>> getEntitySetData(
            UUID entitySetId,
            EntitySetSelection req,
            FileType fileType ) {
        return getEntitySetData( entitySetId, req.getSyncIds(), req.getSelectedProperties() );
    }

    private Iterable<SetMultimap<FullQualifiedName, Object>> getEntitySetData(
            UUID entitySetId,
            Optional<Set<UUID>> syncIds,
            Optional<Set<UUID>> selectedProperties ) {
        if ( authz.checkIfHasPermissions( ImmutableList.of( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            Set<UUID> ids = syncIds.or( getLatestSyncIds() );
            Set<UUID> authorizedProperties;
            if ( selectedProperties.isPresent() ) {
                if ( !authzHelper.getAllPropertiesOnEntitySet( entitySetId ).containsAll( selectedProperties.get() ) ) {
                    throw new IllegalArgumentException(
                            "Not all selected properties are property types of the entity set." );
                }
                authorizedProperties = Sets.intersection( selectedProperties.get(),
                        authzHelper.getAuthorizedPropertiesOnEntitySet( entitySetId, EnumSet.of( Permission.READ ) ) );
            } else {
                authorizedProperties = authzHelper.getAuthorizedPropertiesOnEntitySet( entitySetId,
                        EnumSet.of( Permission.READ ) );
            }

            Map<UUID, PropertyType> authorizedPropertyTypes = authorizedProperties.stream()
                    .collect( Collectors.toMap( ptId -> ptId, ptId -> dms.getPropertyType( ptId ) ) );
            return cdm.getEntitySetData( entitySetId, ids, authorizedPropertyTypes );

        } else {
            throw new ForbiddenException( "Insufficient permissions to read the entity set or it doesn't exist." );
        }
    }

    @RequestMapping(
        path = { "/" + CONTROLLER + "/" + ENTITY_DATA + "/" + SET_ID_PATH + SYNC_ID_PATH },
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void createEntityData(
            @PathVariable( SET_ID ) UUID entitySetId,
            @PathVariable( SYNC_ID ) UUID syncId,
            @RequestBody Map<String, SetMultimap<UUID, Object>> entities ) {
        if ( authz.checkIfHasPermissions( ImmutableList.of( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.WRITE ) ) ) {
            Set<UUID> authorizedProperties = authzHelper.getAuthorizedPropertiesOnEntitySet( entitySetId,
                    EnumSet.of( Permission.WRITE ) );

            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType = authorizedProperties.stream()
                    .collect( Collectors.toMap( ptId -> ptId, ptId -> dms.getPropertyType( ptId ).getDatatype() ) );

            cdm.createEntityData( entitySetId, syncId, entities, authorizedPropertiesWithDataType );
        } else {
            throw new ForbiddenException( "Insufficient permissions to write to the entity set or it doesn't exist." );
        }
        return null;
    }

    private Set<UUID> getLatestSyncIds() {
        // TODO Should be obtained from DatasourcesApi once that is done.
        throw new NotImplementedException( "Ho Chung should fix this once DatasourcesApi is done" );
    }

    /**
     * Methods for setting http response header
     */

    private static void setDownloadContentType( HttpServletResponse response, FileType fileType ) {
        if ( fileType == FileType.csv ) {
            response.setContentType( CustomMediaType.TEXT_CSV_VALUE );
        } else {
            response.setContentType( MediaType.APPLICATION_JSON_VALUE );
        }
    }

    private static void setContentDisposition(
            HttpServletResponse response,
            String fileName,
            FileType fileType ) {
        if ( fileType == FileType.csv || fileType == FileType.json ) {
            response.setHeader( "Content-Disposition",
                    "attachment; filename=" + fileName + "." + fileType.toString() );
        }
    }

}
