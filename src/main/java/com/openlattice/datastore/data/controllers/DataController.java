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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.transformValues;
import static com.openlattice.authorization.EdmAuthorizationHelper.READ_PERMISSION;
import static com.openlattice.authorization.EdmAuthorizationHelper.WRITE_PERMISSION;
import static com.openlattice.authorization.EdmAuthorizationHelper.aclKeysForAccessCheck;

import com.auth0.spring.security.api.authentication.PreAuthenticatedAuthenticationJsonWebToken;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.openlattice.auditing.AuditEventType;
import com.openlattice.auditing.AuditRecordEntitySetsManager;
import com.openlattice.auditing.AuditableEvent;
import com.openlattice.auditing.AuditingComponent;
import com.openlattice.auditing.S3AuditingService;
import com.openlattice.authorization.AclKey;
import com.openlattice.authorization.AuthorizationManager;
import com.openlattice.authorization.AuthorizingComponent;
import com.openlattice.authorization.EdmAuthorizationHelper;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.Principals;
import com.openlattice.IdConstants;
import com.openlattice.controllers.exceptions.BadRequestException;
import com.openlattice.controllers.exceptions.ForbiddenException;
import com.openlattice.data.CreateAssociationEvent;
import com.openlattice.data.DataApi;
import com.openlattice.data.DataAssociation;
import com.openlattice.data.DataEdge;
import com.openlattice.data.DataEdgeKey;
import com.openlattice.data.DataGraph;
import com.openlattice.data.DataGraphIds;
import com.openlattice.data.DataGraphManager;
import com.openlattice.data.DeleteType;
import com.openlattice.data.EntityDataKey;
import com.openlattice.data.EntitySetData;
import com.openlattice.data.UpdateType;
import com.openlattice.data.WriteEvent;
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
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
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
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
    private S3AuditingService s3AuditingService;

    @Inject
    private SecurePrincipalsManager spm;

    private LoadingCache<UUID, EdmPrimitiveTypeKind> primitiveTypeKinds;

    private LoadingCache<AuthorizationKey, Set<UUID>> authorizedPropertyCache;

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
            HttpServletResponse response ) {
        setContentDisposition( response, entitySetId.toString(), fileType );
        setDownloadContentType( response, fileType );
        return loadEntitySetData( entitySetId, selection, fileType );
    }

    @Override
    public EntitySetData<FullQualifiedName> loadEntitySetData(
            UUID entitySetId,
            EntitySetSelection selection,
            FileType fileType ) {
        return loadEntitySetData( entitySetId, selection );
    }

    private EntitySetData<FullQualifiedName> loadEntitySetData(
            UUID entitySetId,
            EntitySetSelection selection ) {
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

            return dgm.getEntitySetData(
                    entityKeyIdsOfEntitySets,
                    orderedPropertyNames,
                    authorizedPropertyTypesOfEntitySets,
                    entitySet.isLinking() );
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
            @RequestParam( value = TYPE, defaultValue = "Merge" ) UpdateType updateType ) {
        Preconditions.checkNotNull( updateType, "An invalid update type value was specified." );
        ensureReadAccess( new AclKey( entitySetId ) );
        var allAuthorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes( entitySetId, EnumSet.of( Permission.WRITE ) );
        var requiredPropertyTypes = requiredEntitySetPropertyTypes( entities );

        accessCheck( allAuthorizedPropertyTypes, requiredPropertyTypes );

        var authorizedPropertyTypes = Maps.asMap( requiredPropertyTypes, allAuthorizedPropertyTypes::get );

        final AuditEventType auditEventType;
        final WriteEvent writeEvent;

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
            @RequestBody Map<UUID, Map<UUID, Set<Map<ByteBuffer, Object>>>> entities ) {
        ensureReadAccess( new AclKey( entitySetId ) );

        final Set<UUID> requiredPropertyTypes = requiredReplacementPropertyTypes( entities );
        accessCheck( aclKeysForAccessCheck( ImmutableSetMultimap.<UUID, UUID>builder()
                        .putAll( entitySetId, requiredPropertyTypes ).build(),
                WRITE_PERMISSION ) );

        WriteEvent writeEvent = dgm.replacePropertiesInEntities( entitySetId,
                entities,
                edmService.getPropertyTypesAsMap( requiredPropertyTypes ) );

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
        final var srcAssociationEntitySetIds = new HashMap<UUID, Set<UUID>>(); // edge-src
        final var dstAssociationEntitySetIds = new HashMap<UUID, Set<UUID>>(); // edge-dst

        final var entitySetIdChecks = new HashMap<AclKey, EnumSet<Permission>>();
        associations.forEach(
                association -> {
                    final var edgeEntitySetId = association.getEdge().getEntitySetId();
                    final var srcEntitySetId = association.getSrc().getEntitySetId();
                    final var dstEntitySetId = association.getDst().getEntitySetId();

                    entitySetIdChecks.put( new AclKey( edgeEntitySetId ), WRITE_PERMISSION );
                    entitySetIdChecks.put( new AclKey( srcEntitySetId ), WRITE_PERMISSION );
                    entitySetIdChecks.put( new AclKey( dstEntitySetId ), WRITE_PERMISSION );

                    if ( srcAssociationEntitySetIds
                            .putIfAbsent( edgeEntitySetId, Sets.newHashSet( srcEntitySetId ) ) != null ) {
                        srcAssociationEntitySetIds.get( edgeEntitySetId ).add( srcEntitySetId );
                    }

                    if ( dstAssociationEntitySetIds
                            .putIfAbsent( edgeEntitySetId, Sets.newHashSet( dstEntitySetId ) ) != null ) {
                        dstAssociationEntitySetIds.get( edgeEntitySetId ).add( dstEntitySetId );
                    }
                }
        );

        //Ensure that we have write access to entity sets.
        accessCheck( entitySetIdChecks );

        WriteEvent writeEvent = dgm
                .createAssociations( associations, srcAssociationEntitySetIds, dstAssociationEntitySetIds );

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
            @RequestBody List<Map<UUID, Set<Object>>> entities ) {
        //Ensure that we have read access to entity set metadata.
        ensureReadAccess( new AclKey( entitySetId ) );
        //Load authorized property types
        final Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes( entitySetId, WRITE_PERMISSION );
        Pair<List<UUID>, WriteEvent> entityKeyIdsToWriteEvent = dgm
                .createEntities( entitySetId, entities, authorizedPropertyTypes );
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
            @RequestBody Map<UUID, Set<Object>> entity ) {
        final var entities = ImmutableMap.of( entityKeyId, entity );
        return updateEntitiesInEntitySet( entitySetId, entities, UpdateType.Merge );
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
    public ListMultimap<UUID, UUID> createAssociations( @RequestBody ListMultimap<UUID, DataEdge> associations ) {
        //Ensure that we have read access to entity set metadata.
        associations.keySet().forEach( entitySetId -> ensureReadAccess( new AclKey( entitySetId ) ) );

        //Ensure that we can write properties.
        final SetMultimap<UUID, UUID> requiredPropertyTypes = requiredAssociationPropertyTypes( associations );
        accessCheck( aclKeysForAccessCheck( requiredPropertyTypes, WRITE_PERMISSION ) );

        final Map<UUID, Map<UUID, PropertyType>> authorizedPropertyTypesByEntitySet =
                associations.keySet().stream()
                        .collect( Collectors.toMap( Function.identity(),
                                entitySetId -> authzHelper
                                        .getAuthorizedPropertyTypes( entitySetId, EnumSet.of( Permission.WRITE ) ) ) );

        Map<UUID, CreateAssociationEvent> associationsCreated = dgm
                .createAssociations( associations, authorizedPropertyTypesByEntitySet );

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
            @RequestParam( value = PARTIAL, required = false, defaultValue = "false" ) boolean partial ) {
        associations.keySet().forEach( entitySetId -> ensureReadAccess( new AclKey( entitySetId ) ) );

        //Ensure that we can write properties.
        final SetMultimap<UUID, UUID> requiredPropertyTypes = requiredAssociationPropertyTypes( associations );
        accessCheck( aclKeysForAccessCheck( requiredPropertyTypes, WRITE_PERMISSION ) );

        final Map<UUID, PropertyType> authorizedPropertyTypes = edmService
                .getPropertyTypesAsMap( ImmutableSet.copyOf( requiredPropertyTypes.values() ) );
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
    }

    @Timed
    @Override
    @PostMapping( value = { "/", "" } )
    public DataGraphIds createEntityAndAssociationData( @RequestBody DataGraph data ) {
        final ListMultimap<UUID, UUID> entityKeyIds = ArrayListMultimap.create();
        final ListMultimap<UUID, UUID> associationEntityKeyIds;

        //First create the entities so we have entity key ids to work with
        Multimaps.asMap( data.getEntities() )
                .forEach( ( entitySetId, entities ) ->
                        entityKeyIds.putAll( entitySetId, createEntities( entitySetId, entities ) ) );
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
        final PostgresIterable<DataEdgeKey> associationsEdgeKeys = collectAssociations( entitySetId, entityKeyIds );

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
        final PostgresIterable<DataEdgeKey> associationsEdgeKeys = collectAssociations( entitySetId, entityKeyIds );
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

    private PostgresIterable<DataEdgeKey> collectAssociations( UUID entitySetId, Optional<Set<UUID>> entityKeyIds ) {
        return ( entityKeyIds.isPresent() )
                ? dgm.getEdgesConnectedToEntities( entitySetId, entityKeyIds.get() )
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

        Map<UUID, Set<EntityDataKey>> entitySetIdToEntityDataKeysMap = dgm
                .getEdgesConnectedToEntities( entitySetId, entityKeyIds )
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
            @RequestBody Map<UUID, Set<Object>> entity ) {
        ensureReadAccess( new AclKey( entitySetId ) );
        Map<UUID, PropertyType> authorizedPropertyTypes = authzHelper.getAuthorizedPropertyTypes( entitySetId,
                WRITE_PERMISSION,
                edmService.getPropertyTypesAsMap( entity.keySet() ),
                Principals.getCurrentPrincipals() );

        WriteEvent writeEvent = dgm
                .replaceEntities( entitySetId, ImmutableMap.of( entityKeyId, entity ), authorizedPropertyTypes );

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
            @RequestBody Map<FullQualifiedName, Set<Object>> entityByFqns ) {
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

        // if properties is empty, then the entire entity should be deleted,
        // so the entityKeyId row in the data table should be deleted too
        if ( !properties.isPresent() ) {
            authorizedPropertyTypes.put( IdConstants.ID_ID.getId(), PostgresMetaDataProperties.ID.getPropertyType() );
        }
        
        if ( !authorizedPropertyTypes.keySet().containsAll( requiredProperties ) ) {
            throw new ForbiddenException(
                    "You must have " + propertyPermissionsToCheck.iterator().next() + " permission of all required " +
                            "entity set " + entitySet.getId() + " properties to delete entities from it." );
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

    @NotNull @Override public S3AuditingService getS3AuditingService() {
        return s3AuditingService;
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

}
