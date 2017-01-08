package com.kryptnostic.datastore.data.controllers;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dataloom.authorization.AclKeyPathFragment;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.EdmAuthorizationHelper;
import com.dataloom.authorization.Principals;
import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.authorization.requests.Permission;
import com.dataloom.data.NewDataApi;
import com.dataloom.data.internal.Entity;
import com.dataloom.data.requests.CreateEntityRequest;
import com.dataloom.data.requests.GetEntitySetRequest;
import com.google.common.base.Optional;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.kryptnostic.datastore.cassandra.CassandraPropertyReader;
import com.kryptnostic.datastore.constants.CustomMediaType;
import com.kryptnostic.datastore.constants.DatastoreConstants;
import com.kryptnostic.datastore.exceptions.ForbiddenException;
import com.kryptnostic.datastore.services.CassandraDataManager;
import com.kryptnostic.datastore.services.EdmService;

@RestController
public class NewDataController implements NewDataApi {

    public static enum FileType {
        json,
        csv;
    }

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
    public Iterable<Entity> getEntitySetData(
            @PathVariable( SET_ID ) UUID entitySetId,
            @RequestParam(
                value = DatastoreConstants.FILE_TYPE,
                required = false ) FileType fileType,
            HttpServletResponse response ) {
        setContentDisposition( response, entitySetId.toString(), fileType );
        setDownloadContentType( response, fileType );
        return getEntitySetData( entitySetId );
    }

    @Override
    public Iterable<Entity> getEntitySetData( UUID entitySetId ) {
        return getEntitySetData( entitySetId, Optional.absent(), Optional.absent() );
    }

    @RequestMapping(
        path = { "/" + CONTROLLER + "/" + ENTITY_DATA + "/" + SET_ID_PATH + "/" + GET_DATA_PATH },
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    public Iterable<Entity> getEntitySetData(
            @PathVariable( SET_ID ) UUID entitySetId,
            @RequestBody GetEntitySetRequest req,
            @RequestParam(
                value = DatastoreConstants.FILE_TYPE,
                required = false ) FileType fileType,
            HttpServletResponse response ) {
        setContentDisposition( response, entitySetId.toString(), fileType );
        setDownloadContentType( response, fileType );
        return getEntitySetData( entitySetId, req );
    }

    @Override
    public Iterable<Entity> getEntitySetData(
            UUID entitySetId,
            GetEntitySetRequest req ) {
        return getEntitySetData( entitySetId, req.getSyncIds(), req.getSelectedProperties() );
    }

    private Iterable<Entity> getEntitySetData(
            UUID entitySetId,
            Optional<Set<UUID>> syncIds,
            Optional<Set<UUID>> selectedProperties ) {
        List<AclKeyPathFragment> sop = EdmAuthorizationHelper.getSecurableObjectPath( SecurableObjectType.EntitySet, entitySetId );
        if ( authz.checkIfHasPermissions( sop,
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

            // TODO EdmService should expose Map<UUID, CassandraPropertyReader> as well, which is updated whenever
            // property type is successfully created.
            Map<UUID, CassandraPropertyReader> authorizedPropertyTypes = dms.getPropertyReaders()
                    .getAll( authorizedProperties );
            return cdm.getEntitySetData( entitySetId, ids, authorizedPropertyTypes );

        } else {
            throw new ForbiddenException( "Insufficient permissions to read the entity set or it doesn't exist." );
        }
    }

    @RequestMapping(
        path = { "/" + CONTROLLER + "/" + ENTITY_DATA },
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void createEntityData( @RequestBody CreateEntityRequest req ) {
        List<AclKeyPathFragment> sop = EdmAuthorizationHelper.getSecurableObjectPath( SecurableObjectType.EntitySet,
                req.getEntitySetId() );
        if ( authz.checkIfHasPermissions( sop,
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.WRITE ) ) ) {
            Set<UUID> authorizedProperties = authzHelper.getAuthorizedPropertiesOnEntitySet( req.getEntitySetId(),
                    EnumSet.of( Permission.WRITE ) );

            cdm.createEntityData( req, authorizedProperties );
        } else {
            throw new ForbiddenException( "Insufficient permissions to write to the entity set or it doesn't exist." );
        }
        return null;
    }

    private Set<UUID> getLatestSyncIds() {
        // TODO Where should this be obtained from?
        return null;
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
