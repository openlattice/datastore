package com.dataloom.datastore.data.controllers;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpServerErrorException;

import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.EdmAuthorizationHelper;
import com.dataloom.authorization.ForbiddenException;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principals;
import com.dataloom.data.DataApi;
import com.dataloom.data.requests.EntitySetSelection;
import com.dataloom.datastore.constants.CustomMediaType;
import com.dataloom.datastore.services.CassandraDataManager;
import com.dataloom.datastore.services.SyncTicketService;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.edm.processors.EdmPrimitiveTypeKindGetter;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.kryptnostic.conductor.rpc.UUIDs.Syncs;
import com.kryptnostic.datastore.services.EdmService;
import com.kryptnostic.datastore.util.Util;

@RestController
@RequestMapping( DataApi.CONTROLLER )
public class DataController implements DataApi {
    private static final Logger                       logger = LoggerFactory.getLogger( DataController.class );

    @Inject
    private SyncTicketService                         sts;

    @Inject
    private EdmService                                dms;

    @Inject
    private CassandraDataManager                      cdm;

    @Inject
    private AuthorizationManager                      authz;

    @Inject
    private EdmAuthorizationHelper                    authzHelper;

    private LoadingCache<UUID, EdmPrimitiveTypeKind>  primitiveTypeKinds;
    private LoadingCache<AuthorizationKey, Set<UUID>> authorizedPropertyCache;

    @RequestMapping(
        path = { "/" + ENTITY_DATA + "/" + SET_ID_PATH },
        method = RequestMethod.POST,
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
        return loadEntitySetData( entitySetId, req, fileType );
    }

    @Override
    public Iterable<SetMultimap<FullQualifiedName, Object>> loadEntitySetData(
            UUID entitySetId,
            EntitySetSelection req,
            FileType fileType ) {
        return loadEntitySetData( entitySetId, req.getSyncIds(), req.getSelectedProperties() );
    }

    private Iterable<SetMultimap<FullQualifiedName, Object>> loadEntitySetData(
            UUID entitySetId,
            Optional<Set<UUID>> syncIds,
            Optional<Set<UUID>> selectedProperties ) {
        if ( authz.checkIfHasPermissions( ImmutableList.of( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            Set<UUID> ids;
            if ( !syncIds.isPresent() || syncIds.get().isEmpty() ) {
                ids = getLatestSyncIds();
            } else {
                ids = syncIds.get();
            }
            Set<UUID> authorizedProperties;
            if ( selectedProperties.isPresent() && !selectedProperties.get().isEmpty() ) {
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
        path = { "/" + ENTITY_DATA + "/" + SET_ID_PATH + "/" + SYNC_ID_PATH },
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void createEntityData(
            @PathVariable( SET_ID ) UUID entitySetId,
            @PathVariable( SYNC_ID ) UUID syncId,
            @RequestBody Map<String, SetMultimap<UUID, Object>> entities ) {
        if ( authz.checkIfHasPermissions( ImmutableList.of( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.WRITE ) ) ) {
            // To avoid re-doing authz check more of than once every 250 ms during an integration we cache the
            // results.cd ../
            AuthorizationKey ak = new AuthorizationKey( Principals.getCurrentUser(), entitySetId, syncId );

            Set<UUID> authorizedProperties = authorizedPropertyCache.getUnchecked( ak );

            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType;

            try {
                authorizedPropertiesWithDataType = primitiveTypeKinds
                        .getAll( authorizedProperties );
            } catch ( ExecutionException e ) {
                logger.error( "Unable to load data types for authorized properties." );
                throw new HttpServerErrorException(
                        HttpStatus.NOT_FOUND,
                        "Unable to load data types for authorized properties." );
            }

            cdm.createEntityData( entitySetId, syncId, entities, authorizedPropertiesWithDataType );
        } else {
            throw new ForbiddenException( "Insufficient permissions to write to the entity set or it doesn't exist." );
        }
        return null;
    }

    @PostConstruct
    void initializeLoadingCache() {
        primitiveTypeKinds = CacheBuilder.newBuilder().maximumSize( 60000 )
                .build( new CacheLoader<UUID, EdmPrimitiveTypeKind>() {
                    @Override
                    public EdmPrimitiveTypeKind load( UUID key ) throws Exception {
                        return Util.getSafely( dms.<EdmPrimitiveTypeKind> fromPropertyTypes( ImmutableSet.of( key ),
                                EdmPrimitiveTypeKindGetter.GETTER ), key );
                    }

                    @Override
                    public Map<UUID, EdmPrimitiveTypeKind> loadAll( Iterable<? extends UUID> keys ) throws Exception {
                        return dms.fromPropertyTypes( ImmutableSet.copyOf( keys ), EdmPrimitiveTypeKindGetter.GETTER );
                    };
                } );
        authorizedPropertyCache = CacheBuilder.newBuilder().expireAfterWrite( 250, TimeUnit.MILLISECONDS )
                .build( new CacheLoader<AuthorizationKey, Set<UUID>>() {

                    @Override
                    public Set<UUID> load( AuthorizationKey key ) throws Exception {
                        return authzHelper.getAuthorizedPropertiesOnEntitySet( key.getEntitySetId(),
                                EnumSet.of( Permission.WRITE ) );
                    }
                } );
    }

    private Set<UUID> getLatestSyncIds() {
        // TODO Ho Chung: Should be obtained from DatasourcesApi once that is done.
        return ImmutableSet.of( Syncs.BASE.getSyncId() );
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

    @Override
    @PostMapping( "/" + TICKET + "/" + SET_ID_PATH + "/" + SYNC_ID_PATH )
    public UUID acquireSyncTicket( @PathVariable( SET_ID ) UUID entitySetId, @PathVariable( SYNC_ID ) UUID syncId ) {
        if ( authz.checkIfHasPermissions( ImmutableList.of( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.WRITE ) ) ) {
            AuthorizationKey ak = new AuthorizationKey( Principals.getCurrentUser(), entitySetId, syncId );
            Set<UUID> authorizedProperties = authorizedPropertyCache.getUnchecked( ak );

            return sts.acquireTicket( Principals.getCurrentUser(), entitySetId, authorizedProperties );
        } else {
            throw new ForbiddenException( "Insufficient permissions to write to the entity set or it doesn't exist." );
        }
    }

    @Override
    @DeleteMapping(
        value = "/" + TICKET + "/" + SET_ID_PATH )
    @ResponseStatus( HttpStatus.OK )
    public Void releaseSyncTicket( @PathVariable( TICKET ) UUID ticketId ) {
        sts.releaseTicket( Principals.getCurrentUser(), ticketId );
        return null;
    }

    @Override
    @RequestMapping(
        value = "/" + ENTITY_DATA + "/" + TICKET_PATH + "/" + SYNC_ID_PATH,
        method = RequestMethod.PATCH,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void storeEntityData(
            @PathVariable( TICKET ) UUID ticket,
            @PathVariable( SYNC_ID ) UUID syncId,
            @RequestBody Map<String, SetMultimap<UUID, Object>> entities ) {

        // To avoid re-doing authz check more of than once every 250 ms during an integration we cache the
        // results.cd ../
        UUID entitySetId = sts.getAuthorizedEntitySet( Principals.getCurrentUser(), ticket );
        Set<UUID> authorizedProperties = sts.getAuthorizedProperties( Principals.getCurrentUser(), ticket );
        Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType;
        try {
            authorizedPropertiesWithDataType = primitiveTypeKinds
                    .getAll( authorizedProperties );
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load data types for authorized properties." );
            throw new HttpServerErrorException(
                    HttpStatus.NOT_FOUND,
                    "Unable to load data types for authorized properties." );
        }

        cdm.createEntityData( entitySetId, syncId, entities, authorizedPropertiesWithDataType );
        return null;
    }

}
