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
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.*;
import com.openlattice.IdConstants;
import com.openlattice.auditing.*;
import com.openlattice.authorization.*;
import com.openlattice.controllers.exceptions.BadRequestException;
import com.openlattice.controllers.exceptions.ForbiddenException;
import com.openlattice.data.*;
import com.openlattice.data.graph.DataGraphServiceHelper;
import com.openlattice.data.requests.EntitySetSelection;
import com.openlattice.data.requests.FileType;
import com.openlattice.datastore.services.EdmService;
import com.openlattice.datastore.services.SyncTicketService;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.set.EntitySetFlag;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.openlattice.organizations.roles.SecurePrincipalsManager;
import com.openlattice.postgres.PostgresMetaDataProperties;
import com.openlattice.postgres.streams.PostgresIterable;
import com.openlattice.search.requests.EntityNeighborsFilter;
import com.openlattice.web.mediatypes.CustomMediaType;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.transformValues;
import static com.openlattice.authorization.EdmAuthorizationHelper.*;

@RestController
@RequestMapping( DataApi.CONTROLLER )
public class DataController implements DataApi, AuthorizingComponent, AuditingComponent {
    private static final Logger logger = LoggerFactory.getLogger( DataController.class );

    @Inject
    private SyncTicketService sts;

    @Inject
    private EdmService edmService;

    @Inject
    private DataGraphManager dgm;

    @Inject
    private AuthorizationManager authz;

    @Inject
    private EdmAuthorizationHelper authzHelper;

    @Inject
    private AuthenticationManager authProvider;

    @Inject
    private AuditRecordEntitySetsManager auditRecordEntitySetsManager;

    @Inject
    private AuditingManager auditingManager;

    @Inject
    private SecurePrincipalsManager spm;

    @Inject
    private DataGraphServiceHelper dataGraphServiceHelper;

    private LoadingCache<UUID, EdmPrimitiveTypeKind> primitiveTypeKinds;

    private LoadingCache<AuthorizationKey, Set<UUID>> authorizedPropertyCache;

    private Semaphore connectionLimiter;

    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH },
            method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    @Timed
    public EntitySetData<FullQualifiedName> loadEntitySetData(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestParam(
                    value = FILE_TYPE,
                    required = false ) FileType fileType,
            @RequestParam(
                    value = TOKEN,
                    required = false ) String token,
            HttpServletResponse response ) throws InterruptedException {
        setContentDisposition( response, entitySetId.toString(), fileType );
        setDownloadContentType( response, fileType );

       return loadEntitySetData( entitySetId, fileType, token );

    }

    @Override
    public EntitySetData<FullQualifiedName> loadEntitySetData(
            UUID entitySetId,
            FileType fileType,
            String token ) throws InterruptedException {
        if ( StringUtils.isNotBlank( token ) ) {
            Authentication authentication = authProvider
                    .authenticate( PreAuthenticatedAuthenticationJsonWebToken.usingToken( token ) );
            SecurityContextHolder.getContext().setAuthentication( authentication );
        }
        return loadEntitySetData( entitySetId, new EntitySetSelection( Optional.empty() ) );
    }

    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH },
            method = RequestMethod.POST,
            consumes = { MediaType.APPLICATION_JSON_VALUE },
            produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    @Timed
    public EntitySetData<FullQualifiedName> loadEntitySetData(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody( required = false ) EntitySetSelection selection,
            @RequestParam( value = FILE_TYPE, required = false ) FileType fileType,
            HttpServletResponse response ) throws InterruptedException {
        setContentDisposition( response, entitySetId.toString(), fileType );
        setDownloadContentType( response, fileType );

        return loadEntitySetData( entitySetId, selection, fileType );
    }

    @Override
    public EntitySetData<FullQualifiedName> loadEntitySetData(
            UUID entitySetId,
            EntitySetSelection selection,
            FileType fileType ) throws InterruptedException {
        return loadEntitySetData( entitySetId, selection );
    }

    private EntitySetData<FullQualifiedName> loadEntitySetData(
            UUID entitySetId,
            EntitySetSelection selection ) throws InterruptedException {
        if ( authz.checkIfHasPermissions(
                new AclKey( entitySetId ), Principals.getCurrentPrincipals(), READ_PERMISSION ) ) {

            Optional<Set<UUID>> entityKeyIds = ( selection == null ) ? Optional.empty() : selection.getEntityKeyIds();
            Optional<Set<UUID>> propertyTypeIds = ( selection == null ) ? Optional.empty() : selection.getProperties();

            final Set<UUID> allProperties = authzHelper.getAllPropertiesOnEntitySet( entitySetId );
            final Set<UUID> selectedProperties = propertyTypeIds.orElse( allProperties );
            checkState( allProperties.equals( selectedProperties ) || allProperties.containsAll( selectedProperties ),
                    "Selected properties are not property types of entity set %s", entitySetId );

            final var entitySet = edmService.getEntitySet( entitySetId );
            Set<UUID> normalEntitySetIds;
            Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesOfEntitySets;

            if ( entitySet.isLinking() ) {
                normalEntitySetIds = Sets.newHashSet( entitySet.getLinkedEntitySets() );
                checkState( !normalEntitySetIds.isEmpty(),
                        "Linked entity sets are empty for linking entity set %s", entitySetId );

                normalEntitySetIds.forEach( esId -> ensureReadAccess( new AclKey( esId ) ) );

                authorizedPropertyTypesOfEntitySets = authzHelper
                        .getAuthorizedPropertyTypesByNormalEntitySet( entitySet, selectedProperties, READ_PERMISSION );

            } else {
                normalEntitySetIds = Set.of( entitySetId );
                authorizedPropertyTypesOfEntitySets = authzHelper
                        .getAuthorizedPropertyTypes( normalEntitySetIds, selectedProperties, READ_PERMISSION );
            }

            final Map<UUID, Optional<Set<UUID>>> entityKeyIdsOfEntitySets = normalEntitySetIds.stream()
                    .collect( Collectors.toMap( esId -> esId, esId -> entityKeyIds ) );

            final var authorizedPropertyTypes = authorizedPropertyTypesOfEntitySets.values().iterator().next();
            final LinkedHashSet<String> orderedPropertyNames = new LinkedHashSet<>( authorizedPropertyTypes.size() );
            selectedProperties.stream()
                    .filter( authorizedPropertyTypes::containsKey )
                    .map( authorizedPropertyTypes::get )
                    .map( pt -> pt.getType().getFullQualifiedNameAsString() )
                    .forEach( orderedPropertyNames::add );

            connectionLimiter.acquire();

            try {

                return dgm.getEntitySetData(
                        entityKeyIdsOfEntitySets,
                        orderedPropertyNames,
                        authorizedPropertyTypesOfEntitySets,
                        entitySet.isLinking() );
            } finally {
                connectionLimiter.release();
            }
        } else {
            throw new ForbiddenException( "Insufficient permissions to read the entity set " + entitySetId
                    + " or it doesn't exists." );
        }
    }

    @Override
    @PutMapping(
            value = "/" + ENTITY_SET + "/" + SET_ID_PATH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @Timed
    public Integer updateEntitiesInEntitySet(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody Map<UUID, Map<UUID, Set<Object>>> entities,
            @RequestParam( value = TYPE, defaultValue = "Merge" ) UpdateType updateType ) throws InterruptedException {
        Preconditions.checkNotNull( updateType, "An invalid update type value was specified." );
        ensureReadAccess( new AclKey( entitySetId ) );
        var allAuthorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes( entitySetId, EnumSet.of( Permission.WRITE ) );
        var requiredPropertyTypes = requiredEntitySetPropertyTypes( entities );

        accessCheck( allAuthorizedPropertyTypes, requiredPropertyTypes );

        var authorizedPropertyTypes = Maps.asMap( requiredPropertyTypes, allAuthorizedPropertyTypes::get );

        final AuditEventType auditEventType;
        final WriteEvent writeEvent;

        connectionLimiter.acquire();

        try {

            switch ( updateType ) {
                case Replace:
                    auditEventType = AuditEventType.REPLACE_ENTITIES;
                    writeEvent = dgm.replaceEntities( entitySetId, entities, authorizedPropertyTypes );
                    break;
                case PartialReplace:
                    auditEventType = AuditEventType.PARTIAL_REPLACE_ENTITIES;
                    writeEvent = dgm.partialReplaceEntities( entitySetId, entities, authorizedPropertyTypes );
                    break;
                case Merge:
                    auditEventType = AuditEventType.MERGE_ENTITIES;
                    writeEvent = dgm.mergeEntities( entitySetId, entities, authorizedPropertyTypes );
                    break;
                default:
                    throw new BadRequestException( "Unsupported UpdateType: \"" + updateType + "\'" );
            }
        } finally {
            connectionLimiter.release();
        }

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                auditEventType,
                "Entities updated using update type " + updateType.toString()
                        + " through DataApi.updateEntitiesInEntitySet",
                Optional.of( entities.keySet() ),
                ImmutableMap.of(),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) );

        return writeEvent.getNumUpdates();
    }

    @PatchMapping(
            value = "/" + ENTITY_SET + "/" + SET_ID_PATH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    @Override
    @Timed
    public Integer replaceEntityProperties(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody Map<UUID, Map<UUID, Set<Map<ByteBuffer, Object>>>> entities ) throws InterruptedException {
        ensureReadAccess( new AclKey( entitySetId ) );

        final Set<UUID> requiredPropertyTypes = requiredReplacementPropertyTypes( entities );
        accessCheck( aclKeysForAccessCheck( ImmutableSetMultimap.<UUID, UUID>builder()
                        .putAll( entitySetId, requiredPropertyTypes ).build(),
                WRITE_PERMISSION ) );

        connectionLimiter.acquire();

        WriteEvent writeEvent;

        try {
            writeEvent = dgm.replacePropertiesInEntities( entitySetId,
                    entities,
                    edmService.getPropertyTypesAsMap( requiredPropertyTypes ) );
        } finally {
            connectionLimiter.release();
        }

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.REPLACE_PROPERTIES_OF_ENTITIES,
                "Entity properties replaced through DataApi.replaceEntityProperties",
                Optional.of( entities.keySet() ),
                ImmutableMap.of(),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) );

        return writeEvent.getNumUpdates();
    }

    @Override
    @Timed
    @PutMapping( value = "/" + ASSOCIATION, consumes = MediaType.APPLICATION_JSON_VALUE )
    public Integer createAssociations( @RequestBody Set<DataEdgeKey> associations ) {
        final var entitySetIdChecks = new HashMap<AclKey, EnumSet<Permission>>();
        associations.forEach(
                association -> {
                    entitySetIdChecks.put( new AclKey( association.getEdge().getEntitySetId() ), WRITE_PERMISSION );
                    entitySetIdChecks.put( new AclKey( association.getSrc().getEntitySetId() ), WRITE_PERMISSION );
                    entitySetIdChecks.put( new AclKey( association.getDst().getEntitySetId() ), WRITE_PERMISSION );
                }
        );

        //Ensure that we have write access to entity sets.
        accessCheck( entitySetIdChecks );

        //Allowed entity types check
        dataGraphServiceHelper.checkEdgeEntityTypes( associations );

        WriteEvent writeEvent = dgm.createAssociations( associations );

        Stream<Pair<EntityDataKey, Map<String, Object>>> neighborMappingsCreated = associations.stream()
                .flatMap( dataEdgeKey -> Stream.of(
                        Pair.of( dataEdgeKey.getSrc(),
                                ImmutableMap.of( "association",
                                        dataEdgeKey.getEdge(),
                                        "neighbor",
                                        dataEdgeKey.getDst(),
                                        "isSrc",
                                        true ) ),
                        Pair.of( dataEdgeKey.getDst(),
                                ImmutableMap.of( "association",
                                        dataEdgeKey.getEdge(),
                                        "neighbor",
                                        dataEdgeKey.getSrc(),
                                        "isSrc",
                                        false ) ),
                        Pair.of( dataEdgeKey.getEdge(),
                                ImmutableMap.of( "src", dataEdgeKey.getSrc(), "dst", dataEdgeKey.getDst() ) )
                ) );

        recordEvents( neighborMappingsCreated.map( pair -> new AuditableEvent(
                getCurrentUserId(),
                new AclKey( pair.getKey().getEntitySetId() ),
                AuditEventType.ASSOCIATE_ENTITIES,
                "Create associations between entities using DataApi.createAssociations",
                Optional.of( ImmutableSet.of( pair.getKey().getEntityKeyId() ) ),
                pair.getValue(),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) ).collect( Collectors.toList() ) );

        return writeEvent.getNumUpdates();
    }

    @Timed
    @Override
    @RequestMapping(
            value = "/" + ENTITY_SET + "/",
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public List<UUID> createEntities(
            @RequestParam( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody List<Map<UUID, Set<Object>>> entities ) throws InterruptedException {
        //Ensure that we have read access to entity set metadata.
        ensureReadAccess( new AclKey( entitySetId ) );
        //Load authorized property types
        final Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes( entitySetId, WRITE_PERMISSION );

        connectionLimiter.acquire();

        Pair<List<UUID>, WriteEvent> entityKeyIdsToWriteEvent;
        try {
            entityKeyIdsToWriteEvent = dgm
                    .createEntities( entitySetId, entities, authorizedPropertyTypes );
        } finally {
            connectionLimiter.release();
        }
        List<UUID> entityKeyIds = entityKeyIdsToWriteEvent.getKey();

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.CREATE_ENTITIES,
                "Entities created through DataApi.createEntities",
                Optional.of( Sets.newHashSet( entityKeyIds ) ),
                ImmutableMap.of(),
                getDateTimeFromLong( entityKeyIdsToWriteEvent.getValue().getVersion() ),
                Optional.empty()
        ) );

        return entityKeyIds;
    }

    @Timed
    @Override
    @PutMapping(
            value = "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public Integer mergeIntoEntityInEntitySet(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @RequestBody Map<UUID, Set<Object>> entity ) throws InterruptedException {
        final var entities = ImmutableMap.of( entityKeyId, entity );
        connectionLimiter.acquire();
        try {
            return updateEntitiesInEntitySet( entitySetId, entities, UpdateType.Merge );
        } finally {
            connectionLimiter.release();
        }
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authz;
    }

    @Timed
    @Override
    @RequestMapping(
            path = { "/" + ASSOCIATION },
            method = RequestMethod.POST,
            consumes = MediaType.APPLICATION_JSON_VALUE )
    public ListMultimap<UUID, UUID> createAssociations( @RequestBody ListMultimap<UUID, DataEdge> associations )
            throws InterruptedException {
        //Ensure that we have read access to entity set metadata.
        associations.keySet().forEach( entitySetId -> ensureReadAccess( new AclKey( entitySetId ) ) );

        //Ensure that we can write properties.
        final SetMultimap<UUID, UUID> requiredPropertyTypes = requiredAssociationPropertyTypes( associations );
        accessCheck( aclKeysForAccessCheck( requiredPropertyTypes, WRITE_PERMISSION ) );

        final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySet = authzHelper
                .getAuthorizedPropertiesOnEntitySets( associations.keySet(), WRITE_PERMISSION );

        dataGraphServiceHelper.checkAssociationEntityTypes( associations );

        connectionLimiter.acquire();
        Map<UUID, CreateAssociationEvent> associationsCreated;
        try {
            associationsCreated = dgm
                    .createAssociations( associations, authorizedPropertyTypesByEntitySet );
        } finally {
            connectionLimiter.release();
        }

        ListMultimap<UUID, UUID> associationIds = ArrayListMultimap.create();

        UUID currentUserId = getCurrentUserId();

        Stream<AuditableEvent> associationEntitiesCreated = associationsCreated.entrySet().stream().map( entry -> {
            UUID associationEntitySetId = entry.getKey();
            OffsetDateTime writeDateTime = getDateTimeFromLong( entry.getValue().getEntityWriteEvent()
                    .getVersion() );
            associationIds.putAll( associationEntitySetId, entry.getValue().getIds() );

            return new AuditableEvent(
                    currentUserId,
                    new AclKey( associationEntitySetId ),
                    AuditEventType.CREATE_ENTITIES,
                    "Create association entities using DataApi.createAssociations",
                    Optional.of( Sets.newHashSet( entry.getValue().getIds() ) ),
                    ImmutableMap.of(),
                    writeDateTime,
                    Optional.empty()
            );
        } );

        Stream<AuditableEvent> neighborMappingsCreated = associationsCreated
                .entrySet()
                .stream()
                .flatMap( entry -> {
                    UUID associationEntitySetId = entry.getKey();
                    OffsetDateTime writeDateTime = getDateTimeFromLong( entry.getValue().getEdgeWriteEvent()
                            .getVersion() );

                    return Streams.mapWithIndex( entry.getValue().getIds().stream(),
                            ( associationEntityKeyId, index ) -> {

                                EntityDataKey associationEntityDataKey = new EntityDataKey( associationEntitySetId,
                                        associationEntityKeyId );
                                DataEdge dataEdge = associations.get( associationEntitySetId )
                                        .get( Long.valueOf( index ).intValue() );

                                return Stream.<Triple<EntityDataKey, OffsetDateTime, Map<String, Object>>>of(
                                        Triple.of( dataEdge.getSrc(),
                                                writeDateTime,
                                                ImmutableMap.of( "association",
                                                        associationEntityDataKey,
                                                        "neighbor",
                                                        dataEdge.getDst(),
                                                        "isSrc",
                                                        true ) ),
                                        Triple.of( dataEdge.getDst(),
                                                writeDateTime,
                                                ImmutableMap.of( "association",
                                                        associationEntityDataKey,
                                                        "neighbor",
                                                        dataEdge.getSrc(),
                                                        "isSrc",
                                                        false ) ),
                                        Triple.of( associationEntityDataKey,
                                                writeDateTime,
                                                ImmutableMap.of( "src",
                                                        dataEdge.getSrc(),
                                                        "dst",
                                                        dataEdge.getDst() ) ) );
                            } );
                } ).flatMap( Function.identity() ).map( triple -> new AuditableEvent(
                        currentUserId,
                        new AclKey( triple.getLeft().getEntitySetId() ),
                        AuditEventType.ASSOCIATE_ENTITIES,
                        "Create associations between entities using DataApi.createAssociations",
                        Optional.of( ImmutableSet.of( triple.getLeft().getEntityKeyId() ) ),
                        triple.getRight(),
                        triple.getMiddle(),
                        Optional.empty()
                ) );

        recordEvents( Stream.concat( associationEntitiesCreated, neighborMappingsCreated )
                .collect( Collectors.toList() ) );

        return associationIds;
    }

    @Timed
    @Override
    @PatchMapping( value = "/" + ASSOCIATION )
    public Integer replaceAssociationData(
            @RequestBody Map<UUID, Map<UUID, DataEdge>> associations,
            @RequestParam( value = PARTIAL, required = false, defaultValue = "false" ) boolean partial )
            throws InterruptedException {
        associations.keySet().forEach( entitySetId -> ensureReadAccess( new AclKey( entitySetId ) ) );

        //Ensure that we can write properties.
        final SetMultimap<UUID, UUID> requiredPropertyTypes = requiredAssociationPropertyTypes( associations );
        accessCheck( aclKeysForAccessCheck( requiredPropertyTypes, WRITE_PERMISSION ) );

        final Map<UUID, PropertyType> authorizedPropertyTypes = edmService
                .getPropertyTypesAsMap( ImmutableSet.copyOf( requiredPropertyTypes.values() ) );

        connectionLimiter.acquire();
        try {
            return associations.entrySet().stream().mapToInt( association -> {
                final UUID entitySetId = association.getKey();
                if ( partial ) {
                    return dgm.partialReplaceEntities( entitySetId,
                            transformValues( association.getValue(), DataEdge::getData ),
                            authorizedPropertyTypes ).getNumUpdates();
                } else {

                    return dgm.replaceEntities( entitySetId,
                            transformValues( association.getValue(), DataEdge::getData ),
                            authorizedPropertyTypes ).getNumUpdates();
                }
            } ).sum();
        } finally {
            connectionLimiter.release();
        }
    }

    @Timed
    @Override
    @PostMapping( value = { "/", "" } )
    public DataGraphIds createEntityAndAssociationData( @RequestBody DataGraph data ) throws InterruptedException {
        final ListMultimap<UUID, UUID> entityKeyIds = ArrayListMultimap.create();
        final ListMultimap<UUID, UUID> associationEntityKeyIds;

        //First create the entities so we have entity key ids to work with
        for ( var entry : Multimaps.asMap( data.getEntities() ).entrySet() ) {
            entityKeyIds.putAll( entry.getKey(), createEntities( entry.getKey(), entry.getValue() ) );
        }

        final ListMultimap<UUID, DataEdge> toBeCreated = ArrayListMultimap.create();
        Multimaps.asMap( data.getAssociations() )
                .forEach( ( entitySetId, associations ) -> {
                    for ( DataAssociation association : associations ) {
                        final UUID srcEntitySetId = association.getSrcEntitySetId();
                        final UUID srcEntityKeyId = association
                                .getSrcEntityKeyId()
                                .orElseGet( () ->
                                        entityKeyIds.get( srcEntitySetId )
                                                .get( association.getSrcEntityIndex().get() ) );

                        final UUID dstEntitySetId = association.getDstEntitySetId();
                        final UUID dstEntityKeyId = association
                                .getDstEntityKeyId()
                                .orElseGet( () ->
                                        entityKeyIds.get( dstEntitySetId )
                                                .get( association.getDstEntityIndex().get() ) );

                        toBeCreated.put(
                                entitySetId,
                                new DataEdge(
                                        new EntityDataKey( srcEntitySetId, srcEntityKeyId ),
                                        new EntityDataKey( dstEntitySetId, dstEntityKeyId ),
                                        association.getData() ) );
                    }
                } );
        associationEntityKeyIds = createAssociations( toBeCreated );

        /* entity and association creation will be audited by createEntities and createAssociations */

        return new DataGraphIds( entityKeyIds, associationEntityKeyIds );
    }

    @Timed
    @Override
    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ALL },
            method = RequestMethod.DELETE )
    public Integer deleteAllEntitiesFromEntitySet(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestParam( value = TYPE ) DeleteType deleteType ) {
        WriteEvent writeEvent;
        // access checks to entity set and property types
        final Map<UUID, PropertyType> authorizedPropertyTypes =
                getAuthorizedPropertyTypesForDelete( entitySetId, Optional.empty(), deleteType );

        if ( deleteType == DeleteType.Hard ) {
            // associations need to be deleted first, because edges are deleted in DataGraphManager.deleteEntitySet call
            deleteAssociations( entitySetId, Optional.empty() );
            writeEvent = dgm.deleteEntitySet( entitySetId, authorizedPropertyTypes );
        } else {
            // associations need to be cleared first, because edges are cleared in DataGraphManager.clearEntitySet call
            clearAssociations( entitySetId, Optional.empty() );
            writeEvent = dgm.clearEntitySet( entitySetId, authorizedPropertyTypes );
        }

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.DELETE_ENTITIES,
                "All entities deleted from entity set using delete type " + deleteType.toString()
                        + " through DataApi.deleteAllEntitiesFromEntitySet",
                Optional.empty(),
                ImmutableMap.of(),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) );

        return writeEvent.getNumUpdates();
    }

    @Timed
    @Override
    @DeleteMapping( path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH } )
    public Integer deleteEntity(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @RequestParam( value = TYPE ) DeleteType deleteType ) {
        return deleteEntities( entitySetId, ImmutableSet.of( entityKeyId ), deleteType );
    }

    @Timed
    @Override
    @DeleteMapping( path = { "/" + ENTITY_SET + "/" + SET_ID_PATH } )
    public Integer deleteEntities(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody Set<UUID> entityKeyIds,
            @RequestParam( value = TYPE ) DeleteType deleteType ) {

        WriteEvent writeEvent = clearOrDeleteEntities( entitySetId, entityKeyIds, deleteType );

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.DELETE_ENTITIES,
                "Entities deleted using delete type " + deleteType.toString() + " through DataApi.deleteEntities",
                Optional.of( entityKeyIds ),
                ImmutableMap.of(),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) );

        return writeEvent.getNumUpdates();
    }

    @Timed
    @Override
    @DeleteMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH + "/" + PROPERTIES } )
    public Integer deleteEntityProperties(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @RequestBody Set<UUID> propertyTypeIds,
            @RequestParam( value = TYPE ) DeleteType deleteType ) {
        WriteEvent writeEvent;

        // access checks for entity set and properties
        final Map<UUID, PropertyType> authorizedPropertyTypes =
                getAuthorizedPropertyTypesForDelete( entitySetId, Optional.of( propertyTypeIds ), deleteType );

        if ( deleteType == DeleteType.Hard ) {
            writeEvent = dgm
                    .deleteEntityProperties( entitySetId, ImmutableSet.of( entityKeyId ), authorizedPropertyTypes );
        } else {
            writeEvent = dgm
                    .clearEntityProperties( entitySetId, ImmutableSet.of( entityKeyId ), authorizedPropertyTypes );
        }

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.DELETE_PROPERTIES_OF_ENTITIES,
                "Entity properties deleted using delete type " + deleteType.toString()
                        + " through DataApi.deleteEntityProperties",
                Optional.of( ImmutableSet.of( entityKeyId ) ),
                ImmutableMap.of( "propertyTypeIds", propertyTypeIds ),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) );

        return writeEvent.getNumUpdates();
    }

    private List<WriteEvent> clearAssociations( UUID entitySetId, Optional<Set<UUID>> entityKeyIds ) {
        final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = new HashMap<>();

        // collect association entity key ids
        final PostgresIterable<DataEdgeKey> associationsEdgeKeys = collectAssociations( entitySetId,
                entityKeyIds,
                false );

        // collect edge entity sets
        final var edgeEntitySetIds = associationsEdgeKeys.stream()
                .map( edgeKey -> edgeKey.getEdge().getEntitySetId() )
                .collect( Collectors.toSet() );
        final var auditEdgeEntitySetIds = edmService
                .getEntitySetIdsWithFlags( edgeEntitySetIds, Set.of( EntitySetFlag.AUDIT ) );

        final var filteredAssociationsEdgeKeys = associationsEdgeKeys.stream()
                // for soft deletes, we skip clearing edge audit entities
                // filter out audit entity sets
                .filter( edgeKey -> !auditEdgeEntitySetIds.contains( edgeKey.getEdge().getEntitySetId() ) )
                // access checks
                .peek( edgeKey -> {
                    if ( !authorizedPropertyTypes.containsKey( edgeKey.getEdge().getEntitySetId() ) ) {
                        Map<UUID, PropertyType> authorizedPropertyTypesOfAssociation =
                                getAuthorizedPropertyTypesForDelete(
                                        edgeKey.getEdge().getEntitySetId(), Optional.empty(), DeleteType.Soft );
                        authorizedPropertyTypes.put(
                                edgeKey.getEdge().getEntitySetId(), authorizedPropertyTypesOfAssociation );
                    }
                } )
                .collect( Collectors.toList() );

        // clear associations of entity set
        return dgm.clearAssociationsBatch( entitySetId, filteredAssociationsEdgeKeys, authorizedPropertyTypes );
    }

    private List<WriteEvent> deleteAssociations( UUID entitySetId, Optional<Set<UUID>> entityKeyIds ) {
        // collect association entity key ids
        final PostgresIterable<DataEdgeKey> associationsEdgeKeys = collectAssociations( entitySetId,
                entityKeyIds,
                true );
        final var edgeAuditPropertyTypes = edmService.getPropertyTypesAsMap(
                edmService.getAuditRecordEntitySetsManager().getAuditingTypes().edgeEntityType
                        .getAssociationEntityType().getProperties() );

        // collect edge entity sets
        final var edgeEntitySetIds = associationsEdgeKeys.stream()
                .map( edgeKey -> edgeKey.getEdge().getEntitySetId() )
                .collect( Collectors.toSet() );
        final var edgeEntitySets = edmService.getEntitySetsAsMap( edgeEntitySetIds );

        // access checks
        final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = new HashMap<>();
        associationsEdgeKeys.stream().forEach( edgeKey -> {
                    if ( !authorizedPropertyTypes.containsKey( edgeKey.getEdge().getEntitySetId() ) ) {
                        // for hard deletes, we skip permission checks for edge audit entity sets, so we can delete
                        // those entries
                        final var edgeEntitySet = edgeEntitySets.get( edgeKey.getEdge().getEntitySetId() );
                        Map<UUID, PropertyType> authorizedPropertyTypesOfAssociation =
                                ( edgeEntitySet.getEntityTypeId().equals(
                                        edmService.getAuditRecordEntitySetsManager().getAuditingTypes()
                                                .auditingEdgeEntityTypeId ) )
                                        ? edgeAuditPropertyTypes
                                        : getAuthorizedPropertyTypesForDelete(
                                        edgeEntitySet, Optional.empty(), DeleteType.Hard );
                        authorizedPropertyTypes.put(
                                edgeKey.getEdge().getEntitySetId(), authorizedPropertyTypesOfAssociation );
                    }
                }
        );

        // delete associations of entity set
        return dgm.deleteAssociationsBatch( entitySetId, associationsEdgeKeys, authorizedPropertyTypes );
    }

    private PostgresIterable<DataEdgeKey> collectAssociations(
            UUID entitySetId,
            Optional<Set<UUID>> entityKeyIds,
            boolean includeClearedEdges ) {
        return ( entityKeyIds.isPresent() )
                ? dgm.getEdgesConnectedToEntities( entitySetId, entityKeyIds.get(), includeClearedEdges )
                : dgm.getEdgeKeysOfEntitySet( entitySetId );
    }

    @Timed
    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + NEIGHBORS },
            method = RequestMethod.POST )
    public Long deleteEntitiesAndNeighbors(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @RequestBody EntityNeighborsFilter filter,
            @RequestParam( value = TYPE ) DeleteType deleteType ) {
        // Note: this function is only useful for deleting src/dst entities and their neighboring entities
        // (along with associations connected to all of them), not associations.
        // If called with an association entity set, it will simplify down to a basic delete call.

        final Set<UUID> entityKeyIds = filter.getEntityKeyIds();

        // we don't include associations in filtering, since they will be deleted anyways with deleting the entities
        final Set<UUID> filteringNeighborEntitySetIds = Stream
                .of( filter.getSrcEntitySetIds().orElse( Set.of() ), filter.getDstEntitySetIds().orElse( Set.of() ) )
                .flatMap( Set::stream )
                .collect( Collectors.toSet() );

        // if no neighbor entity set ids are defined to delete from, it reduces down to a simple deleteEntities call
        if ( filteringNeighborEntitySetIds.isEmpty() ) {
            return Integer.valueOf( clearOrDeleteEntities( entitySetId, entityKeyIds, deleteType ).getNumUpdates() )
                    .longValue();
        }

        /*
         * 1 - collect all neighbor entities, organized by EntitySet
         */

        boolean includeClearedEdges = deleteType.equals( DeleteType.Hard );
        Map<UUID, Set<EntityDataKey>> entitySetIdToEntityDataKeysMap = dgm
                .getEdgesConnectedToEntities( entitySetId, entityKeyIds, includeClearedEdges )
                .stream()
                .filter( edge ->
                        ( edge.getDst().getEntitySetId().equals( entitySetId )
                                && filteringNeighborEntitySetIds.contains( edge.getSrc().getEntitySetId() ) )
                                || ( edge.getSrc().getEntitySetId().equals( entitySetId )
                                && filteringNeighborEntitySetIds.contains( edge.getDst().getEntitySetId() ) ) )
                .flatMap( edge -> Stream.of( edge.getSrc(), edge.getDst() ) )
                .collect( Collectors.groupingBy( EntityDataKey::getEntitySetId, Collectors.toSet() ) );

        /*
         * 2 - delete all entities
         */

        /* Delete entity */

        long numUpdates = 0;

        WriteEvent writeEvent = clearOrDeleteEntities( entitySetId, entityKeyIds, deleteType );
        numUpdates += writeEvent.getNumUpdates();

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.DELETE_ENTITY_AND_NEIGHBORHOOD,
                "Entities and all neighbors deleted using delete type " + deleteType.toString() +
                        " through DataApi.clearEntityAndNeighborEntities",
                Optional.of( entityKeyIds ),
                ImmutableMap.of(),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) );

        /* 3 - Delete neighbors */

        List<AuditableEvent> neighborDeleteEvents = Lists.newArrayList();

        numUpdates += entitySetIdToEntityDataKeysMap.entrySet().stream().mapToInt( entry -> {
                    final UUID neighborEntitySetId = entry.getKey();
                    final Set<UUID> neighborEntityKeyIds = entry.getValue().stream().map( EntityDataKey::getEntityKeyId )
                            .collect( Collectors.toSet() );

                    WriteEvent neighborWriteEvent = clearOrDeleteEntities( neighborEntitySetId,
                            neighborEntityKeyIds,
                            deleteType );

                    neighborDeleteEvents.add( new AuditableEvent(
                            getCurrentUserId(),
                            new AclKey( neighborEntitySetId ),
                            AuditEventType.DELETE_ENTITY_AS_PART_OF_NEIGHBORHOOD,
                            "Entity deleted using delete type " + deleteType.toString() + " as part of " +
                                    "neighborhood delete through DataApi.clearEntityAndNeighborEntities",
                            Optional.of( neighborEntityKeyIds ),
                            ImmutableMap.of(),
                            getDateTimeFromLong( neighborWriteEvent.getVersion() ),
                            Optional.empty()
                    ) );

                    return neighborWriteEvent.getNumUpdates();
                }
        ).sum();

        recordEvents( neighborDeleteEvents );

        return numUpdates;

    }

    @Timed
    @Override
    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH },
            method = RequestMethod.PUT )
    public Integer replaceEntityInEntitySet(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @RequestBody Map<UUID, Set<Object>> entity ) throws InterruptedException {
        ensureReadAccess( new AclKey( entitySetId ) );
        Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper.getAuthorizedPropertyTypes( entitySetId,
                WRITE_PERMISSION,
                edmService.getPropertyTypesAsMap( entity.keySet() ),
                Principals.getCurrentPrincipals() );

        connectionLimiter.acquire();
        WriteEvent writeEvent;
        try {
            writeEvent = dgm
                    .replaceEntities( entitySetId, ImmutableMap.of( entityKeyId, entity ), authorizedPropertyTypes );
        } finally {
            connectionLimiter.release();
        }

        recordEvent( new AuditableEvent(
                getCurrentUserId(),
                new AclKey( entitySetId ),
                AuditEventType.REPLACE_ENTITIES,
                "Entity replaced through DataApi.replaceEntityInEntitySet",
                Optional.of( ImmutableSet.of( entityKeyId ) ),
                ImmutableMap.of(),
                getDateTimeFromLong( writeEvent.getVersion() ),
                Optional.empty()
        ) );

        return writeEvent.getNumUpdates();
    }

    @Timed
    @Override
    @RequestMapping(
            path = { "/" + ENTITY_SET + "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH },
            method = RequestMethod.POST )
    public Integer replaceEntityInEntitySetUsingFqns(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @RequestBody Map<FullQualifiedName, Set<Object>> entityByFqns ) throws InterruptedException {
        final Map<UUID, Set<Object>> entity = new HashMap<>();

        entityByFqns
                .forEach( ( fqn, properties ) -> entity.put( edmService.getPropertyTypeId( fqn ), properties ) );

        return replaceEntityInEntitySet( entitySetId, entityKeyId, entity );
    }

    @Timed
    @Override
    @RequestMapping(
            path = { "/" + SET_ID_PATH + "/" + COUNT },
            method = RequestMethod.GET )
    public long getEntitySetSize( @PathVariable( ENTITY_SET_ID ) UUID entitySetId ) {
        ensureReadAccess( new AclKey( entitySetId ) );

        // If entityset is linking: should return distinct count of entities corresponding to the linking entity set,
        // which is the distinct count of linking_id s
        return dgm.getEntitySetSize( entitySetId );
    }

    @Timed
    @Override
    @RequestMapping(
            path = { "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH },
            method = RequestMethod.GET )
    public Map<FullQualifiedName, Set<Object>> getEntity(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        EntitySet entitySet = edmService.getEntitySet( entitySetId );

        if ( entitySet.isLinking() ) {
            checkState( !entitySet.getLinkedEntitySets().isEmpty(),
                    "Linked entity sets are empty for linking entity set %s", entitySetId );
            entitySet.getLinkedEntitySets().forEach( esId -> ensureReadAccess( new AclKey( esId ) ) );

            final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = authzHelper
                    .getAuthorizedPropertiesByNormalEntitySets( entitySet, EnumSet.of( Permission.READ ) );

            return dgm.getLinkingEntity( entitySet.getLinkedEntitySets(), entityKeyId, authorizedPropertyTypes );
        } else {
            final Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper
                    .getAuthorizedPropertyTypes( entitySetId, READ_PERMISSION );
            return dgm.getEntity( entitySetId, entityKeyId, authorizedPropertyTypes );
        }
    }

    @Timed
    @Override
    @GetMapping(
            path = "/" + SET_ID_PATH + "/" + ENTITY_KEY_ID_PATH + "/" + PROPERTY_TYPE_ID_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE )
    public Set<Object> getEntity(
            @PathVariable( ENTITY_SET_ID ) UUID entitySetId,
            @PathVariable( ENTITY_KEY_ID ) UUID entityKeyId,
            @PathVariable( PROPERTY_TYPE_ID ) UUID propertyTypeId ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        final EntitySet entitySet = edmService.getEntitySet( entitySetId );

        if ( entitySet.isLinking() ) {
            checkState( !entitySet.getLinkedEntitySets().isEmpty(),
                    "Linked entity sets are empty for linking entity set %s", entitySetId );

            entitySet.getLinkedEntitySets().forEach( esId -> ensureReadAccess( new AclKey( esId ) ) );

            final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypes = authzHelper
                    .getAuthorizedPropertyTypesByNormalEntitySet(
                            entitySet,
                            Set.of( propertyTypeId ),
                            EnumSet.of( Permission.READ ) );

            // if any of its normal entitysets don't have read permission on property type, reading is not allowed
            if ( authorizedPropertyTypes.values().iterator().next().isEmpty() ) {
                throw new ForbiddenException( "Not authorized to read property type " + propertyTypeId
                        + " in one or more normal entity sets of linking entity set " + entitySetId );
            }

            final var propertyTypeFqn = authorizedPropertyTypes.values().iterator().next().get( propertyTypeId )
                    .getType();

            return dgm.getLinkingEntity(
                    entitySet.getLinkedEntitySets(),
                    entityKeyId,
                    authorizedPropertyTypes )
                    .get( propertyTypeFqn );
        } else {
            ensureReadAccess( new AclKey( entitySetId, propertyTypeId ) );
            final Map<UUID, PropertyType> authorizedPropertyTypes = edmService
                    .getPropertyTypesAsMap( ImmutableSet.of( propertyTypeId ) );

            final var propertyTypeFqn = authorizedPropertyTypes.get( propertyTypeId ).getType();

            return dgm.getEntity( entitySetId, entityKeyId, authorizedPropertyTypes )
                    .get( propertyTypeFqn );
        }
    }

    private Map<UUID, PropertyType> getAuthorizedPropertyTypesForDelete(
            UUID entitySetId,
            Optional<Set<UUID>> properties,
            DeleteType deleteType ) {
        final EntitySet entitySet = edmService.getEntitySet( entitySetId );
        return getAuthorizedPropertyTypesForDelete( entitySet, properties, deleteType );
    }

    private Map<UUID, PropertyType> getAuthorizedPropertyTypesForDelete(
            EntitySet entitySet,
            Optional<Set<UUID>> properties,
            DeleteType deleteType ) {
        final var entitySetId = entitySet.getId();

        EnumSet<Permission> propertyPermissionsToCheck;
        if ( deleteType == DeleteType.Hard ) {
            ensureOwnerAccess( new AclKey( entitySetId ) );
            propertyPermissionsToCheck = EnumSet.of( Permission.OWNER );
        } else {
            ensureReadAccess( new AclKey( entitySetId ) );
            propertyPermissionsToCheck = EnumSet.of( Permission.WRITE );
        }

        if ( entitySet.isLinking() ) {
            throw new IllegalArgumentException( "You cannot delete entities from a linking entity set." );
        }

        final EntityType entityType = edmService.getEntityType( entitySet.getEntityTypeId() );
        final Set<UUID> requiredProperties = properties.orElse( entityType.getProperties() );
        final Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes( ImmutableSet.of( entitySetId ),
                        requiredProperties,
                        propertyPermissionsToCheck )
                .get( entitySetId );

        if ( !authorizedPropertyTypes.keySet().containsAll( requiredProperties ) ) {
            throw new ForbiddenException(
                    "You must have " + propertyPermissionsToCheck.iterator().next() + " permission of all required " +
                            "entity set " + entitySet.getId() + " properties to delete entities from it." );
        }

        // if we delete all properties, also delete @id
        if ( properties.isEmpty() ) {
            authorizedPropertyTypes.put( IdConstants.ID_ID.getId(), PostgresMetaDataProperties.ID.getPropertyType() );
        }

        return authorizedPropertyTypes;
    }

    private UUID getCurrentUserId() {
        return spm.getPrincipal( Principals.getCurrentUser().getId() ).getId();
    }

    private WriteEvent clearOrDeleteEntities( UUID entitySetId, Set<UUID> entityKeyIds, DeleteType deleteType ) {
        int numUpdates = 0;
        long maxVersion = Long.MIN_VALUE;

        final var isAssociationEntitySet = edmService.isAssociationEntitySet( entitySetId );

        // access checks for entity set and properties
        final Map<UUID, PropertyType> authorizedPropertyTypes =
                getAuthorizedPropertyTypesForDelete( entitySetId, Optional.empty(), deleteType );

        Iterable<List<UUID>> entityKeyIdChunks = Iterables.partition( entityKeyIds, MAX_BATCH_SIZE );
        for ( List<UUID> chunkList : entityKeyIdChunks ) {
            Set<UUID> chunk = Sets.newHashSet( chunkList );

            WriteEvent writeEvent;

            if ( deleteType == DeleteType.Hard ) {
                if ( !isAssociationEntitySet ) {
                    deleteAssociations( entitySetId, Optional.of( chunk ) );
                }
                writeEvent = dgm.deleteEntities(
                        entitySetId,
                        chunk,
                        authorizedPropertyTypes );
            } else {
                if ( !isAssociationEntitySet ) {
                    clearAssociations( entitySetId, Optional.of( chunk ) );
                }
                writeEvent = dgm.clearEntities(
                        entitySetId,
                        chunk,
                        authorizedPropertyTypes );
            }

            numUpdates += writeEvent.getNumUpdates();
            maxVersion = Math.max( maxVersion, writeEvent.getVersion() );
        }

        return new WriteEvent( maxVersion, numUpdates );
    }

    @NotNull @Override public AuditingManager getAuditingManager() {
        return auditingManager;
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

    private static SetMultimap<UUID, UUID> requiredAssociationPropertyTypes( ListMultimap<UUID, DataEdge> associations ) {
        final SetMultimap<UUID, UUID> propertyTypesByEntitySet = HashMultimap.create();
        associations.entries().forEach( entry -> propertyTypesByEntitySet
                .putAll( entry.getKey(), entry.getValue().getData().keySet() ) );
        return propertyTypesByEntitySet;
    }

    private static SetMultimap<UUID, UUID> requiredAssociationPropertyTypes( Map<UUID, Map<UUID, DataEdge>> associations ) {
        final SetMultimap<UUID, UUID> propertyTypesByEntitySet = HashMultimap.create();
        associations.forEach( ( esId, edges ) -> edges.values()
                .forEach( de -> propertyTypesByEntitySet.putAll( esId, de.getData().keySet() ) ) );
        return propertyTypesByEntitySet;
    }

    private static Set<UUID> requiredEntitySetPropertyTypes( Map<UUID, Map<UUID, Set<Object>>> entities ) {
        return entities.values().stream().map( Map::keySet ).flatMap( Set::stream )
                .collect( Collectors.toSet() );
    }

    private static Set<UUID> requiredReplacementPropertyTypes( Map<UUID, Map<UUID, Set<Map<ByteBuffer, Object>>>> entities ) {
        return entities.values().stream().flatMap( m -> m.keySet().stream() ).collect( Collectors.toSet() );
    }

    private static OffsetDateTime getDateTimeFromLong( long epochTime ) {
        return OffsetDateTime.ofInstant( Instant.ofEpochMilli( epochTime ), ZoneId.systemDefault() );
    }

    @Inject
    private void setConnectionLimiter( HikariDataSource hds ) {
        this.connectionLimiter = new Semaphore( Math.max( 2, hds.getMaximumPoolSize() - 2 ) );
    }

}
