/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.dataloom.datastore.data.controllers;

import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpServerErrorException;

import com.auth0.jwt.internal.org.apache.commons.lang3.StringUtils;
import com.auth0.spring.security.api.Auth0JWTToken;
import com.dataloom.authentication.LoomAuth0AuthenticationProvider;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.authorization.EdmAuthorizationHelper;
import com.dataloom.authorization.ForbiddenException;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principals;
import com.dataloom.data.DataApi;
import com.dataloom.data.DataGraphManager;
import com.dataloom.data.DatasourceManager;
import com.dataloom.data.EntitySetData;
import com.dataloom.data.requests.Association;
import com.dataloom.data.requests.BulkDataCreation;
import com.dataloom.data.requests.EntitySetSelection;
import com.dataloom.data.requests.FileType;
import com.dataloom.datastore.constants.CustomMediaType;
import com.dataloom.datastore.services.SyncTicketService;
import com.dataloom.edm.processors.EdmPrimitiveTypeKindGetter;
import com.dataloom.edm.type.PropertyType;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.kryptnostic.datastore.exceptions.ResourceNotFoundException;
import com.kryptnostic.datastore.services.EdmService;
import com.kryptnostic.datastore.util.Util;

@RestController
@RequestMapping( DataApi.CONTROLLER )
public class DataController implements DataApi, AuthorizingComponent {
    private static final Logger                       logger = LoggerFactory.getLogger( DataController.class );

    @Inject
    private SyncTicketService                         sts;

    @Inject
    private EdmService                                dms;

    @Inject
    private DataGraphManager                          dgm;

    @Inject
    private AuthorizationManager                      authz;

    @Inject
    private EdmAuthorizationHelper                    authzHelper;

    @Inject
    private LoomAuth0AuthenticationProvider           authProvider;

    @Inject
    private DatasourceManager                         datasourceManager;

    private LoadingCache<UUID, EdmPrimitiveTypeKind>  primitiveTypeKinds;
    private LoadingCache<AuthorizationKey, Set<UUID>> authorizedPropertyCache;

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

    @RequestMapping(
        path = { "/" + ENTITY_DATA + "/" + SET_ID_PATH },
        method = RequestMethod.GET,
        produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    public EntitySetData<FullQualifiedName> loadEntitySetData(
            @PathVariable( SET_ID ) UUID entitySetId,
            @RequestParam(
                value = FILE_TYPE,
                required = false ) FileType fileType,
            @RequestParam(
                value = TOKEN,
                required = false ) String token,
            HttpServletResponse response ) {
        setContentDisposition( response, entitySetId.toString(), fileType );
        setDownloadContentType( response, fileType );

        return loadEntitySetData( entitySetId, fileType, token );
    }

    @Override
    public EntitySetData<FullQualifiedName> loadEntitySetData(
            UUID entitySetId,
            FileType fileType,
            String token ) {
        if ( StringUtils.isNotBlank( token ) ) {
            Authentication authentication = authProvider.authenticate( new Auth0JWTToken( token ) );
            SecurityContextHolder.getContext().setAuthentication( authentication );
        }
        return loadEntitySetData( entitySetId, Optional.absent(), Optional.absent() );
    }

    @RequestMapping(
        path = { "/" + ENTITY_DATA + "/" + SET_ID_PATH },
        method = RequestMethod.POST,
        produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    public EntitySetData<FullQualifiedName> loadEntitySetData(
            @PathVariable( SET_ID ) UUID entitySetId,
            @RequestBody(
                required = false ) EntitySetSelection req,
            @RequestParam(
                value = FILE_TYPE,
                required = false ) FileType fileType,
            HttpServletResponse response ) {
        setContentDisposition( response, entitySetId.toString(), fileType );
        setDownloadContentType( response, fileType );
        return loadEntitySetData( entitySetId, req, fileType );
    }

    @Override
    public EntitySetData<FullQualifiedName> loadEntitySetData(
            UUID entitySetId,
            EntitySetSelection req,
            FileType fileType ) {
        if ( req == null ) {
            return loadEntitySetData( entitySetId, Optional.absent(), Optional.absent() );
        } else {
            return loadEntitySetData( entitySetId, req.getSyncId(), req.getSelectedProperties() );
        }
    }

    private EntitySetData<FullQualifiedName> loadEntitySetData(
            UUID entitySetId,
            Optional<UUID> syncId,
            Optional<Set<UUID>> selectedProperties ) {
        if ( authz.checkIfHasPermissions( ImmutableList.of( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            return loadNormalEntitySetData( entitySetId, syncId, selectedProperties );
        } else {
            throw new ForbiddenException( "Insufficient permissions to read the entity set or it doesn't exist." );
        }
    }

    private EntitySetData<FullQualifiedName> loadNormalEntitySetData(
            UUID entitySetId,
            Optional<UUID> syncId,
            Optional<Set<UUID>> selectedProperties ) {
        UUID id = ( syncId.isPresent() ) ? syncId.get() : datasourceManager.getCurrentSyncId( entitySetId );
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

        LinkedHashSet<String> orderedPropertyNames = authzHelper.getAllPropertiesOnEntitySet( entitySetId )
                .stream()
                .filter( authorizedProperties::contains )
                .map( ptId -> dms.getPropertyType( ptId ).getType() )
                .map( fqn -> fqn.toString() )
                .collect( Collectors.toCollection( () -> new LinkedHashSet<>() ) );
        Map<UUID, PropertyType> authorizedPropertyTypes = authorizedProperties.stream()
                .collect( Collectors.toMap( ptId -> ptId, ptId -> dms.getPropertyType( ptId ) ) );
        return dgm.getEntitySetData( entitySetId, id, orderedPropertyNames, authorizedPropertyTypes );
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

            Set<UUID> keyProperties = dms.getEntityTypeByEntitySetId( entitySetId ).getKey();

            if ( !authorizedProperties.containsAll( keyProperties ) ) {
                throw new ForbiddenException(
                        "Insufficient permissions to write to some of the key property types of the entity set." );
            }

            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType;

            try {
                authorizedPropertiesWithDataType = primitiveTypeKinds
                        .getAll( authorizedProperties );
            } catch ( ExecutionException e ) {
                logger.error(
                        "Unable to load data types for authorized properties for user " + Principals.getCurrentUser()
                                + " and entity set " + entitySetId + "." );
                throw new ResourceNotFoundException( "Unable to load data types for authorized properties." );
            }
            try {
                dgm.createEntities( entitySetId, syncId, entities, authorizedPropertiesWithDataType );
            } catch ( ExecutionException | InterruptedException e ) {
                logger.error( "Unable to bulk create entity data.", e );
                throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() );
            }
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
                    }

            ;
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

    @Override
    @PostMapping( "/" + TICKET + "/" + SET_ID_PATH + "/" + SYNC_ID_PATH )
    public UUID acquireSyncTicket( @PathVariable( SET_ID ) UUID entitySetId, @PathVariable( SYNC_ID ) UUID syncId ) {
        if ( authz.checkIfHasPermissions( ImmutableList.of( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.WRITE ) ) ) {
            AuthorizationKey ak = new AuthorizationKey( Principals.getCurrentUser(), entitySetId, syncId );
            Set<UUID> authorizedProperties = authorizedPropertyCache.getUnchecked( ak );

            Set<UUID> keyProperties = dms.getEntityTypeByEntitySetId( entitySetId ).getKey();

            if ( authorizedProperties.containsAll( keyProperties ) ) {
                return sts.acquireTicket( Principals.getCurrentUser(), entitySetId, authorizedProperties );
            } else {
                throw new ForbiddenException(
                        "Insufficient permissions to write to some of the key property types of the entity set." );
            }
        } else {
            throw new ForbiddenException( "Insufficient permissions to write to the entity set or it doesn't exist." );
        }
    }

    @Override
    @DeleteMapping(
        value = "/" + TICKET + "/" + TICKET_PATH )
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
            logger.error( "Unable to load data types for authorized properties for user " + Principals.getCurrentUser()
                    + " and entity set " + entitySetId + "." );
            throw new ResourceNotFoundException( "Unable to load data types for authorized properties." );
        }
        try {
            dgm.createEntities( entitySetId, syncId, entities, authorizedPropertiesWithDataType );
        } catch ( ExecutionException | InterruptedException e ) {
            logger.error( "Unable to bulk store entity data.", e );
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() );
        }
        return null;
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authz;
    }

    @Override
    @RequestMapping(
        path = { "/" + ASSOCIATION_DATA + "/" + SET_ID_PATH + "/" + SYNC_ID_PATH },
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void createAssociationData(
            @PathVariable( SET_ID ) UUID entitySetId,
            @PathVariable( SYNC_ID ) UUID syncId,
            @RequestBody Set<Association> associations ) {
        if ( authz.checkIfHasPermissions( ImmutableList.of( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.WRITE ) ) ) {
            // To avoid re-doing authz check more of than once every 250 ms during an integration we cache the
            // results.cd ../
            AuthorizationKey ak = new AuthorizationKey( Principals.getCurrentUser(), entitySetId, syncId );

            Set<UUID> authorizedProperties = authorizedPropertyCache.getUnchecked( ak );

            Set<UUID> keyProperties = dms.getEntityTypeByEntitySetId( entitySetId ).getKey();

            if ( !authorizedProperties.containsAll( keyProperties ) ) {
                throw new ForbiddenException(
                        "Insufficient permissions to write to some of the key property types of the entity set." );
            }

            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType;

            try {
                authorizedPropertiesWithDataType = primitiveTypeKinds
                        .getAll( authorizedProperties );
            } catch ( ExecutionException e ) {
                logger.error(
                        "Unable to load data types for authorized properties for user " + Principals.getCurrentUser()
                                + " and entity set " + entitySetId + "." );
                throw new ResourceNotFoundException( "Unable to load data types for authorized properties." );
            }
            try {
                dgm.createAssociations( entitySetId, syncId, associations, authorizedPropertiesWithDataType );
            } catch ( ExecutionException | InterruptedException e ) {
                logger.error( "Unable to bulk create association data.", e );
                throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() );
            }
        } else {
            throw new ForbiddenException( "Insufficient permissions to write to the entity set or it doesn't exist." );
        }
        return null;
    }

    @Override
    @RequestMapping(
        value = "/" + ASSOCIATION_DATA + "/" + TICKET_PATH + "/" + SYNC_ID_PATH,
        method = RequestMethod.PATCH,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void storeAssociationData(
            @PathVariable( TICKET ) UUID ticket,
            @PathVariable( SYNC_ID ) UUID syncId,
            @RequestBody Set<Association> associations ) {

        // To avoid re-doing authz check more of than once every 250 ms during an integration we cache the
        // results.cd ../
        UUID entitySetId = sts.getAuthorizedEntitySet( Principals.getCurrentUser(), ticket );
        Set<UUID> authorizedProperties = sts.getAuthorizedProperties( Principals.getCurrentUser(), ticket );
        Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType;
        try {
            authorizedPropertiesWithDataType = primitiveTypeKinds
                    .getAll( authorizedProperties );
        } catch ( ExecutionException e ) {
            logger.error( "Unable to load data types for authorized properties for user " + Principals.getCurrentUser()
                    + " and entity set " + entitySetId + "." );
            throw new ResourceNotFoundException( "Unable to load data types for authorized properties." );
        }

        try {
            dgm.createAssociations( entitySetId, syncId, associations, authorizedPropertiesWithDataType );
        } catch ( ExecutionException | InterruptedException e ) {
            logger.error( "Unable to bulk store association data.", e );
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() );
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = { "/" + ENTITY_DATA + "/" + SET_ID_PATH },
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void createEntityData(
            @PathVariable( SET_ID ) UUID entitySetId,
            @RequestBody Map<String, SetMultimap<UUID, Object>> entities ) {
        return createEntityData( entitySetId, datasourceManager.getCurrentSyncId( entitySetId ), entities );
    }

    @Override
    @RequestMapping(
        value = "/" + ENTITY_DATA,
        method = RequestMethod.PATCH,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void createEntityAndAssociationData( @RequestBody BulkDataCreation data ) {
        Map<UUID, Map<UUID, EdmPrimitiveTypeKind>> authorizedPropertiesByEntitySetId = Maps.newHashMap();

        data.getTickets().stream().forEach( ticket -> {
            UUID entitySetId = sts.getAuthorizedEntitySet( Principals.getCurrentUser(), ticket );
            Set<UUID> authorizedProperties = sts.getAuthorizedProperties( Principals.getCurrentUser(), ticket );
            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType;
            try {
                authorizedPropertiesWithDataType = primitiveTypeKinds
                        .getAll( authorizedProperties );
            } catch ( ExecutionException e ) {
                logger.error(
                        "Unable to load data types for authorized properties for user " + Principals.getCurrentUser()
                                + " and entity set " + entitySetId + "." );
                throw new ResourceNotFoundException( "Unable to load data types for authorized properties." );
            }
            authorizedPropertiesByEntitySetId.put( entitySetId, authorizedPropertiesWithDataType );
        } );

        try {
            dgm.createEntitiesAndAssociations( data.getEntities(),
                    data.getAssociations(),
                    authorizedPropertiesByEntitySetId );
        } catch ( ExecutionException | InterruptedException e ) {
            logger.error( "Unable to bulk create data.", e );
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() );
        }
        return null;

    }

}
