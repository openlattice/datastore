/*
 * Copyright (C) 2018. OpenLattice, Inc.
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
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.datastore.data.controllers;

import com.auth0.spring.security.api.authentication.PreAuthenticatedAuthenticationJsonWebToken;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.*;
import com.openlattice.authorization.*;
import com.openlattice.data.*;
import com.openlattice.data.requests.*;
import com.openlattice.datastore.constants.CustomMediaType;
import com.openlattice.datastore.exceptions.ResourceNotFoundException;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.SearchService;
import com.openlattice.datastore.services.SyncTicketService;
import com.openlattice.datastore.util.Util;
import com.openlattice.edm.processors.EdmPrimitiveTypeKindGetter;
import com.openlattice.edm.type.PropertyType;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpServerErrorException;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@RestController
@RequestMapping( DataApi.CONTROLLER )
public class DataController implements DataApi, AuthorizingComponent {
    private static final Logger logger = LoggerFactory.getLogger( DataController.class );

    @Inject
    private SyncTicketService sts;

    @Inject
    private EdmService dms;

    @Inject
    private DataGraphManager dgm;

    @Inject
    private AuthorizationManager authz;

    @Inject
    private EdmAuthorizationHelper authzHelper;

    @Inject
    private AuthenticationManager authProvider;

    @Inject
    private DatasourceManager datasourceManager;

    @Inject
    private SearchService searchService;

    @Inject
    private EntityKeyIdService idService;

    private LoadingCache<UUID, EdmPrimitiveTypeKind>  primitiveTypeKinds;
    private LoadingCache<AuthorizationKey, Set<UUID>> authorizedPropertyCache;

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
            Authentication authentication = authProvider
                    .authenticate( PreAuthenticatedAuthenticationJsonWebToken.usingToken( token ) );
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
        if ( authz.checkIfHasPermissions( new AclKey( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.READ ) ) ) {
            return loadNormalEntitySetData( entitySetId, syncId, selectedProperties );
        } else {
            throw new ForbiddenException( "Insufficient permissions to read the entity set or it doesn't exists." );
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
        if ( authz.checkIfHasPermissions( new AclKey( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.WRITE ) ) ) {
            AuthorizationKey ak = new AuthorizationKey( Principals.getCurrentUser(), entitySetId );

            authzHelper.getAuthorizedPropertiesOnEntitySet( entitySetId, EnumSet.of( Permission.WRITE ) );

            //Check authz
            Set<UUID> authorizedProperties = authzHelper
                    .getAuthorizedPropertiesOnEntitySet( entitySetId, EnumSet.of( Permission.WRITE ) );
            Set<UUID> keyProperties = dms.getEntityTypeByEntitySetId( entitySetId ).getKey();

            if ( !authorizedProperties.containsAll( keyProperties ) ) {
                throw new ForbiddenException( "You shall not pass" );
            }

            Map<UUID, EdmPrimitiveTypeKind> authorizedPropertiesWithDataType;

            try {
                authorizedPropertiesWithDataType = primitiveTypeKinds.getAll( authorizedProperties );
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
            throw new ForbiddenException( "You shall not pass!" );
        }
        return null;
    }

    @PostConstruct
    void initializeLoadingCache() {
        primitiveTypeKinds = CacheBuilder.newBuilder().maximumSize( 60000 )
                .build( new CacheLoader<UUID, EdmPrimitiveTypeKind>() {
                    @Override
                    public EdmPrimitiveTypeKind load( UUID key ) throws Exception {
                        return Util.getSafely( dms.<EdmPrimitiveTypeKind>fromPropertyTypes( ImmutableSet.of( key ),
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
        if ( authz.checkIfHasPermissions( new AclKey( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.WRITE ) ) ) {
            //            AuthorizationKey ak = new AuthorizationKey( Principals.getCurrentUser(), entitySetId, syncId );
            Set<UUID> authorizedProperties = authzHelper
                    .getAuthorizedPropertiesOnEntitySet( entitySetId, EnumSet.of( Permission.WRITE ) );

            Set<UUID> keyProperties = dms.getEntityTypeByEntitySetId( entitySetId ).getKey();

            if ( authorizedProperties.containsAll( keyProperties ) ) {

                return sts.acquireTicket( Principals.getCurrentUser(), entitySetId, authorizedProperties );
            } else {
                throw new ForbiddenException(
                        "Insufficient permissions to write to some of the key property types of the entity set." );
            }
        } else {
            throw new ForbiddenException( "Insufficient permissions to write to the entity set or it doesn't exists." );
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
        if ( authz.checkIfHasPermissions( new AclKey( entitySetId ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.WRITE ) ) ) {
            // To avoid re-doing authz check more of than once every 250 ms during an integration we cache the
            // results.cd ../
            //            AuthorizationKey ak = new AuthorizationKey( Principals.getCurrentUser(), entitySetId, syncId );
            //problem is that we are checking all properties
            Set<UUID> authorizedProperties = authzHelper
                    .getAuthorizedPropertiesOnEntitySet( entitySetId, EnumSet.of( Permission.WRITE ) );

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
            throw new ForbiddenException( "Insufficient permissions to write to the entity set or it doesn't exists." );
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
            path = { "/" + ENTITY_KEY_ID },
            method = RequestMethod.PUT )
    public UUID getEntityKeyId( @RequestBody EntityKey entityKey ) {
        ensureWriteAccess( new AclKey( entityKey.getEntitySetId() ) );
        return idService.getEntityKeyId( entityKey );
    }

    @Override
    @RequestMapping(
            path = { "/" + ENTITY_KEY_ID },
            method = RequestMethod.POST )
    public Map<EntityKey, UUID> getEntityKeyIds( @RequestBody Set<EntityKey> entityKeys ) {
        entityKeys.stream().map( entityKey -> entityKey.getEntitySetId() ).distinct()
                .forEach( entitySetId -> ensureWriteAccess( new AclKey( entitySetId ) ) );
        return idService.getEntityKeyIds( entityKeys );
    }

    private Map<UUID, Map<UUID, EdmPrimitiveTypeKind>> getAuthorizedProperties( Set<UUID> entitySetIds ) {
        Map<UUID, Map<UUID, EdmPrimitiveTypeKind>> authorizedPropertiesByEntitySetId = Maps.newHashMap();

        entitySetIds.stream().forEach( entitySetId -> {
            Set<UUID> authorizedProperties = authzHelper.getAuthorizedPropertiesOnEntitySet( entitySetId, EnumSet.of( Permission.WRITE ) );
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

        return authorizedPropertiesByEntitySetId;
    }

    @Override
    @RequestMapping(
            value = "/" + ENTITY_DATA,
            method = RequestMethod.PATCH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void createEntityAndAssociationData( @RequestBody BulkDataCreation data ) {
        Set<UUID> entitySetIds = data
                .getTickets().stream()
                .map( ticket -> sts.getAuthorizedEntitySet( Principals.getCurrentUser(), ticket ) ).collect(
                        Collectors.toSet() );
        Map<UUID, Map<UUID, EdmPrimitiveTypeKind>> authorizedPropertiesByEntitySetId = getAuthorizedProperties( entitySetIds );

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

    @Override
    @RequestMapping(
            value = { "/" + ENTITY_DATA + "/" + ASSOCIATION_DATA },
            method = RequestMethod.PATCH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void bulkCreateEntityData( @RequestBody CreateDataRequest data ) {
        Map<UUID, Map<UUID, EdmPrimitiveTypeKind>> authorizedPropertiesByEntitySetId = getAuthorizedProperties( data
                .getEntitySetIds() );

        try {
            dgm.bulkCreateEntityData( data.getEntities(),
                    data.getAssociations(),
                    authorizedPropertiesByEntitySetId );
        } catch ( ExecutionException | InterruptedException e ) {
            logger.error( "Unable to bulk create data.", e );
            throw new HttpServerErrorException( HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage() );
        }
        return null;
    }

    @Override
    @RequestMapping(
            path = { "/" + ENTITY_DATA + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH },
            method = RequestMethod.DELETE )
    public Void deleteEntityFromEntitySet(
            @PathVariable( SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId ) {
        ensureWriteAccess( new AclKey( entitySetId ) );
        dms.getEntityTypeByEntitySetId( entitySetId ).getProperties().forEach( propertyTypeId -> {
            ensureWriteAccess( new AclKey( entitySetId, propertyTypeId ) );
        } );

        dgm.deleteEntity( new EntityDataKey( entitySetId, entityKeyId ) );
        return null;
    }

    @Override
    @RequestMapping(
            path = { "/" + ENTITY_DATA + "/" + UPDATE + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH },
            method = RequestMethod.PUT )
    public Void replaceEntityInEntitySet(
            @PathVariable( SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @RequestBody SetMultimap<UUID, Object> entity ) {
        ensureWriteAccess( new AclKey( entitySetId ) );
        Map<UUID, EdmPrimitiveTypeKind> propertyTypes = dms.getEntityTypeByEntitySetId( entitySetId ).getProperties()
                .stream().peek( propertyTypeId -> {
                    ensureWriteAccess( new AclKey( entitySetId, propertyTypeId ) );
                } ).map( propertyTypeId -> dms.getPropertyType( propertyTypeId ) ).collect( Collectors
                        .toMap( propertyType -> propertyType.getId(), propertyType -> propertyType.getDatatype() ) );

        dgm.replaceEntity( new EntityDataKey( entitySetId, entityKeyId ), entity, propertyTypes );
        return null;
    }

    @Override
    @RequestMapping(
            path = { "/" + ENTITY_DATA + "/" + UPDATE + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH },
            method = RequestMethod.POST )
    public Void replaceEntityInEntitySetUsingFqns(
            @PathVariable( SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @RequestBody SetMultimap<FullQualifiedName, Object> entityByFqns ) {
        SetMultimap<UUID, Object> entity = HashMultimap.create();
        entityByFqns.entries()
                .forEach( entry -> entity.put( dms.getPropertyType( entry.getKey() ).getId(), entry.getValue() ) );
        return replaceEntityInEntitySet( entitySetId, entityKeyId, entity );
    }

    @Override
    @RequestMapping(
            path = { "/" + SET_ID_PATH + "/" + COUNT },
            method = RequestMethod.GET )
    public long getEntitySetSize( @PathVariable( SET_ID ) UUID entitySetId ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        return searchService.getEntitySetSize( entitySetId );
    }

    @Override
    @RequestMapping(
            path = { "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH },
            method = RequestMethod.GET )
    public SetMultimap<FullQualifiedName, Object> getEntity(
            @PathVariable( SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        Map<UUID, PropertyType> authorizedPropertyTypes = dms
                .getPropertyTypesAsMap( authzHelper.getAuthorizedPropertiesOnEntitySet( entitySetId,
                        EnumSet.of( Permission.READ ) ) );
        return dgm.getEntity( entityKeyId, authorizedPropertyTypes );
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

    private static Set<UUID> requiredPropertyAuthorizations( Collection<SetMultimap<UUID, Object>> entities ) {
        return entities.stream().map( SetMultimap::keySet ).flatMap( Set::stream ).collect( Collectors.toSet() );
    }
}
