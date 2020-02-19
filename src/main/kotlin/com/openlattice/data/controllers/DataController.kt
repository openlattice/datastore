/*
 * Copyright (C) 2019. OpenLattice, Inc.
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
 *
 */

package com.openlattice.data.controllers

import com.auth0.spring.security.api.authentication.PreAuthenticatedAuthenticationJsonWebToken
import com.codahale.metrics.annotation.Timed
import com.google.common.base.Preconditions
import com.google.common.base.Preconditions.checkState
import com.google.common.collect.*
import com.openlattice.auditing.AuditEventType
import com.openlattice.auditing.AuditableEvent
import com.openlattice.auditing.AuditingComponent
import com.openlattice.auditing.AuditingManager
import com.openlattice.authorization.*
import com.openlattice.controllers.exceptions.BadRequestException
import com.openlattice.controllers.exceptions.ForbiddenException
import com.openlattice.data.*
import com.openlattice.data.DataApi.*
import com.openlattice.data.graph.DataGraphServiceHelper
import com.openlattice.data.requests.EntitySetSelection
import com.openlattice.data.requests.FileType
import com.openlattice.datastore.services.EdmService
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.edm.type.PropertyType
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.search.requests.EntityNeighborsFilter
import com.openlattice.search.requests.EntityNeighborsFilterBulk
import com.openlattice.web.mediatypes.CustomMediaType
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import org.apache.commons.lang3.StringUtils
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.springframework.http.MediaType
import org.springframework.security.authentication.AuthenticationManager
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.web.bind.annotation.*
import java.nio.ByteBuffer
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.*
import javax.inject.Inject
import javax.servlet.http.HttpServletResponse
import kotlin.collections.HashMap

@SuppressFBWarnings(
        value = ["NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"],
        justification = "NPEs are prevented by Preconditions.checkState but SpotBugs doesn't understand this"
)
@RestController
@RequestMapping(CONTROLLER)
class DataController @Inject
constructor(
        private val authz: AuthorizationManager,
        private val authProvider: AuthenticationManager,
        private val authzHelper: EdmAuthorizationHelper,
        private val entitySetService: EntitySetManager,
        private val dgm: DataGraphManager,
        private val dataGraphServiceHelper: DataGraphServiceHelper,
        private val edmService: EdmService,
        private val deletionManager: DataDeletionManager,
        private val spm: SecurePrincipalsManager,
        private val auditingManager: AuditingManager
) : DataApi, AuthorizingComponent, AuditingComponent {

    /* GET */

    @Timed
    @RequestMapping(path = [ENTITY_SET_ID_PATH + COUNT], method = [RequestMethod.GET])
    override fun getEntitySetSize(@PathVariable(ENTITY_SET_ID) entitySetId: UUID): Long {
        ensureReadAccess(AclKey(entitySetId))

        // If entityset is linking: should return distinct count of entities corresponding to the linking entity set,
        // which is the distinct count of linking_id s
        return dgm.getEntitySetSize(entitySetId)
    }

    @Suppress("UNUSED")
    @RequestMapping(
            path = [ENTITY_SET + ENTITY_SET_ID_PATH],
            method = [RequestMethod.GET],
            produces = [MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE])
    @Timed
    fun loadEntitySetData(
            @PathVariable(ENTITY_SET_ID) entitySetId: UUID,
            @RequestParam(value = FILE_TYPE, required = false) fileType: FileType?,
            @RequestParam(value = TOKEN, required = false) token: String?,
            response: HttpServletResponse
    ): EntitySetData<FullQualifiedName> {
        setContentDisposition(response, entitySetId.toString(), fileType)
        setDownloadContentType(response, fileType)

        return loadEntitySetData(entitySetId, fileType, token)
    }

    override fun loadEntitySetData(
            entitySetId: UUID, fileType: FileType?, token: String?
    ): EntitySetData<FullQualifiedName> {
        if (StringUtils.isNotBlank(token)) {
            val authentication = authProvider
                    .authenticate(PreAuthenticatedAuthenticationJsonWebToken.usingToken(token))
            SecurityContextHolder.getContext().authentication = authentication
        }
        return loadEntitySetData(entitySetId, EntitySetSelection(Optional.empty()))
    }

    @Suppress("UNUSED")
    @RequestMapping(
            path = [ENTITY_SET + ENTITY_SET_ID_PATH],
            method = [RequestMethod.POST],
            consumes = [MediaType.APPLICATION_JSON_VALUE],
            produces = [MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE])
    @Timed
    fun loadEntitySetData(
            @PathVariable(ENTITY_SET_ID) entitySetId: UUID,
            @RequestBody(required = false) selection: EntitySetSelection,
            @RequestParam(value = FILE_TYPE, required = false) fileType: FileType?,
            response: HttpServletResponse
    ): EntitySetData<FullQualifiedName> {
        setContentDisposition(response, entitySetId.toString(), fileType)
        setDownloadContentType(response, fileType)

        return loadSelectedEntitySetData(entitySetId, selection, fileType)
    }

    override fun loadSelectedEntitySetData(
            entitySetId: UUID, selection: EntitySetSelection, fileType: FileType?
    ): EntitySetData<FullQualifiedName> {
        return loadEntitySetData(entitySetId, selection)
    }

    private fun loadEntitySetData(entitySetId: UUID, selection: EntitySetSelection?): EntitySetData<FullQualifiedName> {
        if (authz.checkIfHasPermissions(
                        AclKey(entitySetId),
                        Principals.getCurrentPrincipals(),
                        EdmAuthorizationHelper.READ_PERMISSION
                )) {
            val entitySet = getEntitySet(entitySetId)

            val entityKeyIds = if (selection == null) Optional.empty() else selection.entityKeyIds
            val selectedProperties = getSelectedProperties(entitySetId, selection)

            val normalEntitySetIds = if (entitySet.isLinking) entitySet.linkedEntitySets else setOf(entitySetId)

            val authorizedPropertyTypesOfEntitySets = getAuthorizedPropertyTypesForEntitySetRead(
                    entitySet, normalEntitySetIds, selectedProperties
            )

            val authorizedPropertyTypes = authorizedPropertyTypesOfEntitySets.values.first()
            val orderedPropertyNames = LinkedHashSet<String>(authorizedPropertyTypes.size)

            selectedProperties.filter { authorizedPropertyTypes.containsKey(it) }
                    .map { authorizedPropertyTypes.getValue(it).type.fullQualifiedNameAsString }
                    .toCollection(orderedPropertyNames)

            return if (entitySet.isLinking) {
                dgm.getLinkedEntitySetData(
                        entitySet,
                        entityKeyIds,
                        orderedPropertyNames,
                        authorizedPropertyTypesOfEntitySets
                )
            } else {
                dgm.getEntitySetData(
                        mapOf(entitySetId to entityKeyIds),
                        orderedPropertyNames,
                        authorizedPropertyTypesOfEntitySets
                )
            }
        } else {
            throw ForbiddenException(
                    "Insufficient permissions to read the entity set $entitySetId or it doesn't exists."
            )
        }
    }

    @Timed
    @PostMapping(
            path = [ENTITY_SET + ENTITY_SET_ID_PATH + DETAILED],
            consumes = [MediaType.APPLICATION_JSON_VALUE],
            produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun loadLinkedEntitySetBreakdown(
            @PathVariable(ENTITY_SET_ID) linkedEntitySetId: UUID,
            @RequestBody selection: EntitySetSelection?
    ): Map<UUID, Map<UUID, Map<UUID, Map<FullQualifiedName, Set<Any>>>>> {
        ensureReadAccess(AclKey(linkedEntitySetId))

        val entitySet = getEntitySet(linkedEntitySetId)

        val selectedProperties = getSelectedProperties(linkedEntitySetId, selection)
        val authorizedPropertyTypesOfEntitySets = getAuthorizedPropertyTypesForEntitySetRead(
                entitySet, entitySet.linkedEntitySets, selectedProperties
        )

        val entityKeyIds = if (selection == null) Optional.empty() else selection.entityKeyIds

        return dgm.getLinkedEntitySetBreakDown(entitySet, entityKeyIds, authorizedPropertyTypesOfEntitySets)
    }

    @Timed
    @RequestMapping(path = [ENTITY_SET_ID_PATH + ENTITY_KEY_ID_PATH], method = [RequestMethod.GET])
    override fun getEntity(
            @PathVariable(ENTITY_SET_ID) entitySetId: UUID,
            @PathVariable(ENTITY_KEY_ID) entityKeyId: UUID
    ): Map<FullQualifiedName, Set<Any>> {
        ensureReadAccess(AclKey(entitySetId))
        val entitySet = getEntitySet(entitySetId)

        val normalEntitySetIds = if (entitySet.isLinking) entitySet.linkedEntitySets else setOf(entitySetId)
        val properties = authzHelper.getAllPropertiesOnEntitySet(entitySetId)
        val authorizedPropertyTypes = getAuthorizedPropertyTypesForEntitySetRead(
                entitySet, normalEntitySetIds, properties
        )

        return if (entitySet.isLinking) {
            dgm.getLinkingEntity(entitySet, entityKeyId, authorizedPropertyTypes)
        } else {
            dgm.getEntity(entitySetId, entityKeyId, authorizedPropertyTypes.getValue(entitySetId))
        }
    }

    @Timed
    @GetMapping(
            path = [ENTITY_SET_ID_PATH + ENTITY_KEY_ID_PATH + PROPERTY_TYPE_ID_PATH],
            produces = [MediaType.APPLICATION_JSON_VALUE])
    override fun getEntityPropertyValues(
            @PathVariable(ENTITY_SET_ID) entitySetId: UUID,
            @PathVariable(ENTITY_KEY_ID) entityKeyId: UUID,
            @PathVariable(PROPERTY_TYPE_ID) propertyTypeId: UUID
    ): Set<Any> {
        ensureReadAccess(AclKey(entitySetId))
        val entitySet = getEntitySet(entitySetId)

        val normalEntitySetIds = if (entitySet.isLinking) entitySet.linkedEntitySets else setOf(entitySetId)
        val authorizedPropertyTypesByEntitySet = getAuthorizedPropertyTypesForEntitySetRead(
                entitySet, normalEntitySetIds, setOf(propertyTypeId)
        )

        val authorizedPropertyTypes = authorizedPropertyTypesByEntitySet.getValue(normalEntitySetIds.first())
        if (authorizedPropertyTypes.isEmpty()) {
            throw ForbiddenException("Not authorized to read property type $propertyTypeId in entity set $entitySetId.")
        }

        val propertyTypeFqn = authorizedPropertyTypes.getValue(propertyTypeId).type
        return if (entitySet.isLinking) {
            dgm.getLinkingEntity(entitySet, entityKeyId, authorizedPropertyTypesByEntitySet).getValue(propertyTypeFqn)
        } else {
            dgm.getEntity(entitySetId, entityKeyId, authorizedPropertyTypes).getValue(propertyTypeFqn)
        }
    }


    /* CREATE */

    @Timed
    @RequestMapping(value = ["$ENTITY_SET/"], method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun createEntities(
            @RequestParam(ENTITY_SET_ID) entitySetId: UUID,
            @RequestBody entities: List<Map<UUID, Set<Any>>>
    ): List<UUID> {
        //Ensure that we have read access to entity set metadata.
        ensureReadAccess(AclKey(entitySetId))
        ensureEntitySetCanBeWritten(entitySetId)
        val requiredPropertyTypes = entities.flatMap { entity -> entity.keys }.toSet()

        //Load authorized property types
        val authorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes(entitySetId, EdmAuthorizationHelper.WRITE_PERMISSION)
        accessCheck(authorizedPropertyTypes, requiredPropertyTypes)
        val entityKeyIdsToWriteEvent = dgm.createEntities(entitySetId, entities, authorizedPropertyTypes)
        val entityKeyIds = entityKeyIdsToWriteEvent.key

        recordEvent(AuditableEvent(
                spm.getCurrentUserId(),
                AclKey(entitySetId),
                AuditEventType.CREATE_ENTITIES,
                "Entities created through DataApi.createEntities",
                Optional.of<Set<UUID>>(Sets.newHashSet(entityKeyIds)),
                mapOf(),
                entityKeyIdsToWriteEvent.value.version
        ))

        return entityKeyIds
    }

    @Timed
    @RequestMapping(value = [ENTITY], method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun createEntities(@RequestBody entities: Map<UUID, List<Map<UUID, Set<Any>>>>): Map<UUID, List<UUID>> {
        return entities.mapValues { (entitySetId, entityData) ->
            createEntities(entitySetId, entityData)
        }
    }

    @Timed
    @RequestMapping(path = [ASSOCIATION], method = [RequestMethod.POST], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun createAssociations(@RequestBody associations: Map<UUID, List<DataEdge>>): Map<UUID, List<UUID>> {
        //Ensure that we have read access to entity set metadata.
        val entitySetIds = getEntitySetIdsFromCollection(associations.values.flatten()) { dataEdge ->
            listOf(dataEdge.src.entitySetId, dataEdge.dst.entitySetId)
        }
        checkPermissionsOnEntitySetIds(entitySetIds, EdmAuthorizationHelper.READ_PERMISSION)

        //Ensure that we can write properties.
        val requiredPropertyTypes = requiredAssociationPropertyTypes(associations) { it }
        accessCheck(EdmAuthorizationHelper.aclKeysForAccessCheck(
                requiredPropertyTypes, EdmAuthorizationHelper.WRITE_PERMISSION
        ))

        val authorizedPropertyTypesByEntitySet = authzHelper
                .getAuthorizedPropertiesOnEntitySets(associations.keys, EdmAuthorizationHelper.WRITE_PERMISSION)

        dataGraphServiceHelper.checkAssociationEntityTypes(associations)
        val associationsCreated = dgm.createAssociations(associations, authorizedPropertyTypesByEntitySet)

        val associationIds = mutableMapOf<UUID, List<UUID>>()

        val currentUserId = spm.getCurrentUserId()

        val entitiesCreated = mutableListOf<AuditableEvent>()
        associationsCreated.forEach { (associationEntitySetId, createAssociationEvent) ->
            associationIds[associationEntitySetId] = createAssociationEvent.ids

            entitiesCreated.add(
                    AuditableEvent(
                            currentUserId,
                            AclKey(associationEntitySetId),
                            AuditEventType.CREATE_ENTITIES,
                            "Create association entities using DataApi.createAssociations",
                            Optional.of(createAssociationEvent.ids.toSet()),
                            mapOf(),
                            createAssociationEvent.entityWriteEvent.version
                    )
            )
        }

        associationsCreated
                .entries
                .forEach { entry ->
                    val associationEntitySetId = entry.key
                    val writeDateTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(entry.value.edgeWriteEvent
                            .version), ZoneId.systemDefault())

                    entry.value.ids.forEachIndexed { index, associationEntityKeyId ->

                        val associationEntityDataKey = EntityDataKey(associationEntitySetId, associationEntityKeyId)
                        val dataEdge = associations.getValue(associationEntitySetId)[index]

                        entitiesCreated.add(
                                AuditableEvent(
                                        currentUserId,
                                        AclKey(dataEdge.src.entitySetId),
                                        AuditEventType.ASSOCIATE_ENTITIES,
                                        "Create associations between entities using DataApi.createAssociations",
                                        Optional.of(setOf(dataEdge.src.entityKeyId)),
                                        mapOf<String, Any>(
                                                "association" to associationEntityDataKey,
                                                "neighbor" to dataEdge.dst,
                                                "isSrc" to true
                                        ),
                                        writeDateTime,
                                        Optional.empty()
                                ))

                        entitiesCreated.add(
                                AuditableEvent(
                                        currentUserId,
                                        AclKey(dataEdge.dst.entitySetId),
                                        AuditEventType.ASSOCIATE_ENTITIES,
                                        "Create associations between entities using DataApi.createAssociations",
                                        Optional.of(setOf(dataEdge.dst.entityKeyId)),
                                        mapOf<String, Any>(
                                                "association" to associationEntityDataKey,
                                                "neighbor" to dataEdge.src,
                                                "isSrc" to false
                                        ),
                                        writeDateTime,
                                        Optional.empty()
                                ))

                        entitiesCreated.add(
                                AuditableEvent(
                                        currentUserId,
                                        AclKey(associationEntityDataKey.entitySetId),
                                        AuditEventType.ASSOCIATE_ENTITIES,
                                        "Create associations between entities using DataApi.createAssociations",
                                        Optional.of(setOf(associationEntityDataKey.entityKeyId)),
                                        mapOf<String, Any>("src" to dataEdge.src, "dst" to dataEdge.dst),
                                        writeDateTime,
                                        Optional.empty()
                                ))

                    }
                }

        recordEvents(entitiesCreated)

        return associationIds
    }

    @Timed
    @PostMapping(value = [""])
    override fun createEntityAndAssociationData(@RequestBody data: DataGraph): DataGraphIds {
        val entitySetIds = getEntitySetIdsFromCollection<DataAssociation>(data.associations.values.flatten())
        { association -> listOf(association.srcEntitySetId, association.dstEntitySetId) }
        checkPermissionsOnEntitySetIds(entitySetIds, EdmAuthorizationHelper.READ_PERMISSION)

        //First create the entities so we have entity key ids to work with
        val entityKeyIds = createEntities(data.entities)

        val toBeCreated = mutableMapOf<UUID, List<DataEdge>>()
        data.associations.forEach { (entitySetId, associations) ->

            toBeCreated[entitySetId] =
                    associations.map { association ->
                        val srcEntitySetId = association.srcEntitySetId
                        val srcEntityKeyId = association.srcEntityKeyId
                                .orElseGet { entityKeyIds.getValue(srcEntitySetId)[association.srcEntityIndex.get()] }

                        val dstEntitySetId = association.dstEntitySetId
                        val dstEntityKeyId = association.dstEntityKeyId
                                .orElseGet { entityKeyIds.getValue(dstEntitySetId)[association.dstEntityIndex.get()] }


                        DataEdge(
                                EntityDataKey(srcEntitySetId, srcEntityKeyId),
                                EntityDataKey(dstEntitySetId, dstEntityKeyId),
                                association.data
                        )
                    }
        }
        val associationEntityKeyIds = createAssociations(toBeCreated)

        /* entity and association creation will be audited by createEntities and createAssociations */

        return DataGraphIds(entityKeyIds, associationEntityKeyIds)
    }

    @Timed
    @PutMapping(value = [ASSOCIATION], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun createEdges(@RequestBody associations: Set<DataEdgeKey>): Int {

        val entitySetIds = getEntitySetIdsFromCollection(associations) { dataEdgeKey ->
            listOf(dataEdgeKey.edge.entitySetId, dataEdgeKey.src.entitySetId, dataEdgeKey.dst.entitySetId)
        }
        checkPermissionsOnEntitySetIds(entitySetIds, EnumSet.of(Permission.READ, Permission.WRITE))

        //Allowed entity types check
        dataGraphServiceHelper.checkEdgeEntityTypes(associations)

        val writeEvent = dgm.createAssociations(associations)

        val neighborMappingsCreatedEvents = associations
                .flatMap { dataEdgeKey ->
                    listOf(
                            AuditableEvent(
                                    spm.getCurrentUserId(),
                                    AclKey(dataEdgeKey.src.entitySetId),
                                    AuditEventType.ASSOCIATE_ENTITIES,
                                    "Create associations between entities using DataApi.createAssociations",
                                    Optional.of(setOf(dataEdgeKey.src.entityKeyId)),
                                    mapOf<String, Any>(
                                            "association" to dataEdgeKey.edge,
                                            "neighbor" to dataEdgeKey.dst,
                                            "isSrc" to true
                                    ),
                                    writeEvent.version
                            ),
                            AuditableEvent(
                                    spm.getCurrentUserId(),
                                    AclKey(dataEdgeKey.dst.entitySetId),
                                    AuditEventType.ASSOCIATE_ENTITIES,
                                    "Create associations between entities using DataApi.createAssociations",
                                    Optional.of(setOf(dataEdgeKey.dst.entityKeyId)),
                                    mapOf<String, Any>(
                                            "association" to dataEdgeKey.edge,
                                            "neighbor" to dataEdgeKey.src,
                                            "isSrc" to false
                                    ),
                                    writeEvent.version
                            ),
                            AuditableEvent(
                                    spm.getCurrentUserId(),
                                    AclKey(dataEdgeKey.edge.entitySetId),
                                    AuditEventType.ASSOCIATE_ENTITIES,
                                    "Create associations between entities using DataApi.createAssociations",
                                    Optional.of(setOf(dataEdgeKey.edge.entityKeyId)),
                                    mapOf<String, Any>("src" to dataEdgeKey.src, "dst" to dataEdgeKey.dst),
                                    writeEvent.version
                            )
                    )
                }

        recordEvents(neighborMappingsCreatedEvents)

        return writeEvent.numUpdates
    }


    /* UPDATE */


    @PutMapping(value = [ENTITY_SET + ENTITY_SET_ID_PATH], consumes = [MediaType.APPLICATION_JSON_VALUE])
    @Timed
    override fun updateEntitiesInEntitySet(
            @PathVariable(ENTITY_SET_ID) entitySetId: UUID,
            @RequestBody entities: Map<UUID, Map<UUID, Set<Any>>>,
            @RequestParam(value = TYPE, defaultValue = "Merge") updateType: UpdateType
    ): Int {
        Preconditions.checkNotNull(updateType, "An invalid update type value was specified.")
        ensureReadAccess(AclKey(entitySetId))
        ensureEntitySetCanBeWritten(entitySetId)

        val allAuthorizedPropertyTypes = authzHelper
                .getAuthorizedPropertyTypes(entitySetId, EnumSet.of(Permission.WRITE))
        val requiredPropertyTypes = entities.values.flatMap { it.keys }.toSet()

        accessCheck(allAuthorizedPropertyTypes, requiredPropertyTypes)

        val authorizedPropertyTypes = requiredPropertyTypes.map {
            it to allAuthorizedPropertyTypes.getValue(it)
        }.toMap()

        val auditEventType: AuditEventType
        val writeEvent: WriteEvent

        when (updateType) {
            UpdateType.Replace -> {
                auditEventType = AuditEventType.REPLACE_ENTITIES
                writeEvent = dgm.replaceEntities(entitySetId, entities, authorizedPropertyTypes)
            }
            UpdateType.PartialReplace -> {
                auditEventType = AuditEventType.PARTIAL_REPLACE_ENTITIES
                writeEvent = dgm.partialReplaceEntities(entitySetId, entities, authorizedPropertyTypes)
            }
            UpdateType.Merge -> {
                auditEventType = AuditEventType.MERGE_ENTITIES
                writeEvent = dgm.mergeEntities(entitySetId, entities, authorizedPropertyTypes)
            }
            else -> throw BadRequestException("Unsupported UpdateType: \"$updateType\'")
        }

        recordEvent(AuditableEvent(
                spm.getCurrentUserId(),
                AclKey(entitySetId),
                auditEventType,
                "Entities updated using update type $updateType through DataApi.updateEntitiesInEntitySet",
                Optional.of(entities.keys),
                mapOf(),
                writeEvent.version
        ))

        return writeEvent.numUpdates
    }

    @Timed
    @PutMapping(value = [ENTITY_SET + ENTITY_SET_ID_PATH + ENTITY_KEY_ID_PATH], consumes = [MediaType.APPLICATION_JSON_VALUE])
    override fun mergeIntoEntityInEntitySet(
            @PathVariable(ENTITY_SET_ID) entitySetId: UUID,
            @PathVariable(ENTITY_KEY_ID) entityKeyId: UUID,
            @RequestBody entity: Map<UUID, Set<Any>>
    ): Int {
        return updateEntitiesInEntitySet(entitySetId, mapOf(entityKeyId to entity), UpdateType.Merge)
    }

    @Timed
    @RequestMapping(path = [ENTITY_SET + ENTITY_SET_ID_PATH + ENTITY_KEY_ID_PATH], method = [RequestMethod.PUT])
    override fun replaceEntityInEntitySet(
            @PathVariable(ENTITY_SET_ID) entitySetId: UUID,
            @PathVariable(ENTITY_KEY_ID) entityKeyId: UUID,
            @RequestBody entity: Map<UUID, Set<Any>>
    ): Int {
        return updateEntitiesInEntitySet(entitySetId, mapOf(entityKeyId to entity), UpdateType.Replace)
    }


    @PatchMapping(value = [ENTITY_SET + ENTITY_SET_ID_PATH], consumes = [MediaType.APPLICATION_JSON_VALUE])
    @Timed
    override fun replaceEntityProperties(
            @PathVariable(ENTITY_SET_ID) entitySetId: UUID,
            @RequestBody entities: Map<UUID, Map<UUID, Set<Map<ByteBuffer, Any>>>>
    ): Int {
        ensureReadAccess(AclKey(entitySetId))
        ensureEntitySetCanBeWritten(entitySetId)

        val requiredPropertyTypes = entities.values.flatMap { m -> m.keys }.toSet()
        accessCheck(
                EdmAuthorizationHelper.aclKeysForAccessCheck(
                        mapOf(entitySetId to requiredPropertyTypes), EdmAuthorizationHelper.WRITE_PERMISSION
                )
        )

        val writeEvent = dgm.replacePropertiesInEntities(
                entitySetId,
                entities,
                edmService.getPropertyTypesAsMap(requiredPropertyTypes)
        )

        recordEvent(AuditableEvent(
                spm.getCurrentUserId(),
                AclKey(entitySetId),
                AuditEventType.REPLACE_PROPERTIES_OF_ENTITIES,
                "Entity properties replaced through DataApi.replaceEntityProperties",
                Optional.of(entities.keys),
                ImmutableMap.of(),
                writeEvent.version
        ))

        return writeEvent.numUpdates
    }

    @Timed
    @PatchMapping(value = [ASSOCIATION])
    override fun replaceAssociationData(
            @RequestBody associations: Map<UUID, Map<UUID, DataEdge>>,
            @RequestParam(value = PARTIAL, required = false, defaultValue = "false") partial: Boolean
    ): Int {
        associations.keys.forEach { entitySetId -> ensureReadAccess(AclKey(entitySetId)) }

        //Ensure that we can write properties.
        val requiredPropertyTypes = requiredAssociationPropertyTypes(associations) { it.values }
        ensureEntitySetsCanBeWritten(associations.keys)
        accessCheck(EdmAuthorizationHelper.aclKeysForAccessCheck(
                requiredPropertyTypes, EdmAuthorizationHelper.WRITE_PERMISSION
        ))

        val authorizedPropertyTypeIds = mutableSetOf<UUID>()
        requiredPropertyTypes.forEach { authorizedPropertyTypeIds.addAll(it.value) }
        val authorizedPropertyTypes = edmService.getPropertyTypesAsMap(authorizedPropertyTypeIds)

        return associations.entries.map { association ->
            val entitySetId = association.key
            if (partial) {
                dgm.partialReplaceEntities(
                        entitySetId, association.value.mapValues { it.value.data }, authorizedPropertyTypes
                ).numUpdates
            } else {
                dgm.replaceEntities(
                        entitySetId, association.value.mapValues { it.value.data }, authorizedPropertyTypes
                ).numUpdates
            }
        }.sum()
    }

    @Timed
    @RequestMapping(path = [ENTITY_SET + ENTITY_SET_ID_PATH + ENTITY_KEY_ID_PATH], method = [RequestMethod.POST])
    override fun replaceEntityInEntitySetUsingFqns(
            @PathVariable(ENTITY_SET_ID) entitySetId: UUID,
            @PathVariable(ENTITY_KEY_ID) entityKeyId: UUID,
            @RequestBody entityByFqns: Map<FullQualifiedName, Set<Any>>
    ): Int {
        val entity = entityByFqns.map { (fqn, properties) -> edmService.getPropertyTypeId(fqn) to properties }.toMap()

        return replaceEntityInEntitySet(entitySetId, entityKeyId, entity)
    }


    /* DELETE */

    @Timed
    @RequestMapping(path = [ENTITY_SET + ENTITY_SET_ID_PATH + ALL], method = [RequestMethod.DELETE])
    override fun deleteAllEntitiesFromEntitySet(
            @PathVariable(ENTITY_SET_ID) entitySetId: UUID,
            @RequestParam(value = TYPE) deleteType: DeleteType
    ): Int {
        ensureEntitySetCanBeWritten(entitySetId)

        val writeEvent = deletionManager
                .clearOrDeleteEntitySetIfAuthorized(entitySetId, deleteType, Principals.getCurrentPrincipals())

        recordEvent(AuditableEvent(
                spm.getCurrentUserId(),
                AclKey(entitySetId),
                AuditEventType.DELETE_ENTITIES,
                "All entities deleted from entity set using delete type $deleteType through " +
                        "DataApi.deleteAllEntitiesFromEntitySet",
                Optional.empty(),
                mapOf(),
                writeEvent.version
        ))

        return writeEvent.numUpdates
    }

    @Timed
    @DeleteMapping(path = [ENTITY_SET + ENTITY_SET_ID_PATH + ENTITY_KEY_ID_PATH])
    override fun deleteEntity(
            @PathVariable(ENTITY_SET_ID) entitySetId: UUID,
            @PathVariable(ENTITY_KEY_ID) entityKeyId: UUID,
            @RequestParam(value = TYPE) deleteType: DeleteType
    ): Int {
        return deleteEntities(entitySetId, setOf(entityKeyId), deleteType)
    }

    @Timed
    @DeleteMapping(path = [ENTITY_SET + ENTITY_SET_ID_PATH])
    override fun deleteEntities(
            @PathVariable(ENTITY_SET_ID) entitySetId: UUID,
            @RequestBody entityKeyIds: Set<UUID>,
            @RequestParam(value = TYPE) deleteType: DeleteType
    ): Int {
        ensureEntitySetCanBeWritten(entitySetId)

        val writeEvent = deletionManager.clearOrDeleteEntitiesIfAuthorized(
                entitySetId, entityKeyIds, deleteType, Principals.getCurrentPrincipals()
        )

        recordEvent(AuditableEvent(
                spm.getCurrentUserId(),
                AclKey(entitySetId),
                AuditEventType.DELETE_ENTITIES,
                "Entities deleted using delete type $deleteType through DataApi.deleteEntities",
                Optional.of(entityKeyIds),
                mapOf(),
                writeEvent.version
        ))

        return writeEvent.numUpdates
    }

    @Timed
    @DeleteMapping(path = [ENTITY_SET + ENTITY_SET_ID_PATH + ENTITY_KEY_ID_PATH + PROPERTIES])
    override fun deleteEntityProperties(
            @PathVariable(ENTITY_SET_ID) entitySetId: UUID,
            @PathVariable(ENTITY_KEY_ID) entityKeyId: UUID,
            @RequestBody propertyTypeIds: Set<UUID>,
            @RequestParam(value = TYPE) deleteType: DeleteType
    ): Int {
        ensureEntitySetCanBeWritten(entitySetId)

        val writeEvent = deletionManager.clearOrDeleteEntityProperties(
                entitySetId, setOf(entityKeyId), deleteType, propertyTypeIds, Principals.getCurrentPrincipals()
        )

        recordEvent(AuditableEvent(
                spm.getCurrentUserId(),
                AclKey(entitySetId),
                AuditEventType.DELETE_PROPERTIES_OF_ENTITIES,
                "Entity properties deleted using delete type $deleteType through " +
                        "DataApi.deleteEntityProperties",
                Optional.of(setOf(entityKeyId)),
                mapOf("propertyTypeIds" to propertyTypeIds),
                writeEvent.version
        ))

        return writeEvent.numUpdates
    }

    @Timed
    @RequestMapping(path = [ENTITY_SET + NEIGHBORS], method = [RequestMethod.POST])
    override fun deleteEntitiesAndNeighbors(
            @RequestBody filter: EntityNeighborsFilterBulk,
            @RequestParam(value = TYPE) deleteType: DeleteType
    ): Long {
        // Note: this function is only useful for deleting src/dst entities and their neighboring entities
        // (along with associations connected to all of them), not associations.
        // If called with an association entity set, it will simplify down to a basic delete call.
        return filter.entityKeyIds.map { (entitySetId, entityKeyIds) ->
            ensureEntitySetCanBeWritten(entitySetId)

            val writeEvent = deletionManager.clearOrDeleteEntitiesAndNeighborsIfAuthorized(
                    entitySetId,
                    entityKeyIds,
                    filter.srcEntitySetIds.orElse(setOf()),
                    filter.dstEntitySetIds.orElse(setOf()),
                    deleteType,
                    Principals.getCurrentPrincipals()
            )

            recordEvent(AuditableEvent(
                    spm.getCurrentUserId(),
                    AclKey(entitySetId),
                    AuditEventType.DELETE_ENTITY_AND_NEIGHBORHOOD,
                    ("Entities and all neighbors deleted using delete type $deleteType through " +
                            "DataApi.clearEntityAndNeighborEntities"),
                    Optional.of(entityKeyIds),
                    mapOf(),
                    writeEvent.version
            ))

            writeEvent.numUpdates.toLong()
        }.sum()
    }

    @Timed
    @RequestMapping(path = [ENTITY_SET + ENTITY_SET_ID_PATH + NEIGHBORS], method = [RequestMethod.POST])
    override fun deleteEntitiesAndNeighbors(
            @PathVariable entitySetId: UUID,
            @RequestBody filter: EntityNeighborsFilter,
            @RequestParam(value = TYPE) deleteType: DeleteType
    ): Long {
        return deleteEntitiesAndNeighbors(EntityNeighborsFilterBulk(entitySetId, filter), deleteType)
    }


    /* UTIL */

    private fun setContentDisposition(response: HttpServletResponse, fileName: String, fileType: FileType?) {
        if (fileType == FileType.csv || fileType == FileType.json) {
            response.setHeader("Content-Disposition", "attachment; filename=$fileName.$fileType")
        }
    }

    private fun setDownloadContentType(response: HttpServletResponse, fileType: FileType?) {
        if (fileType == FileType.csv) {
            response.contentType = CustomMediaType.TEXT_CSV_VALUE
        } else {
            response.contentType = MediaType.APPLICATION_JSON_VALUE
        }
    }

    private fun getEntitySet(entitySetId: UUID): EntitySet {
        return entitySetService.getEntitySet(entitySetId)
                ?: throw IllegalStateException("Could not find entity set with id: $entitySetId")
    }

    private fun getSelectedProperties(entitySetId: UUID, selection: EntitySetSelection?): Set<UUID> {
        val propertyTypeIds = if (selection == null) Optional.empty() else selection.properties
        val allProperties = authzHelper.getAllPropertiesOnEntitySet(entitySetId)
        val selectedProperties = propertyTypeIds.orElse(allProperties)
        checkState(
                allProperties == selectedProperties || allProperties.containsAll(selectedProperties),
                "Selected properties are not property types of entity set %s",
                entitySetId
        )

        return selectedProperties
    }

    private fun getAuthorizedPropertyTypesForEntitySetRead(
            entitySet: EntitySet,
            normalEntitySetIds: Set<UUID>,
            selectedProperties: Set<UUID>
    ): Map<UUID, Map<UUID, PropertyType>> {
        return if (entitySet.isLinking) {
            checkState(
                    normalEntitySetIds.isNotEmpty(),
                    "Linked entity sets are empty for linking entity set %s",
                    entitySet.id
            )
            normalEntitySetIds.forEach { esId -> ensureReadAccess(AclKey(esId)) }

            authzHelper.getAuthorizedPropertyTypesByNormalEntitySet(
                    entitySet, selectedProperties, EdmAuthorizationHelper.READ_PERMISSION
            )
        } else {
            authzHelper.getAuthorizedPropertyTypes(
                    normalEntitySetIds, selectedProperties, EdmAuthorizationHelper.READ_PERMISSION
            )
        }
    }

    private fun checkPermissionsOnEntitySetIds(entitySetIds: Set<UUID>, permissions: EnumSet<Permission>) {
        //Ensure that we have write access to entity sets.
        ensureEntitySetsCanBeWritten(entitySetIds)
        accessCheck(entitySetIds.map { AclKey(it) to permissions }.toMap())
    }

    private fun ensureEntitySetCanBeWritten(entitySetId: UUID) {
        ensureEntitySetsCanBeWritten(setOf(entitySetId))
    }

    private fun ensureEntitySetsCanBeWritten(entitySetIds: Set<UUID>) {
        if (entitySetService.entitySetsContainFlag(entitySetIds, EntitySetFlag.AUDIT)) {
            val auditEntitySetIds = entitySetService
                    .getEntitySetsWithFlags(entitySetIds, setOf(EntitySetFlag.AUDIT)).keys
            throw ForbiddenException(
                    "You cannot modify data of entity sets $auditEntitySetIds because they are audit entity sets."
            )
        }
    }

    private fun <T> requiredAssociationPropertyTypes(
            associations: Map<UUID, T>,
            valueExtractor: (T) -> Collection<DataEdge>
    ): Map<UUID, MutableSet<UUID>> {
        val propertyTypesByEntitySet = HashMap<UUID, MutableSet<UUID>>(associations.size)
        associations.forEach { (entitySetId, dataEdges) ->
            val propertyTypes = propertyTypesByEntitySet.getOrPut(entitySetId, { mutableSetOf() })

            valueExtractor(dataEdges).forEach { dataEdge ->
                propertyTypes.addAll(dataEdge.data.keys)
            }
        }

        return propertyTypesByEntitySet
    }

    private fun <T> getEntitySetIdsFromCollection(
            items: Collection<T>, transformation: (T) -> Iterable<UUID>
    ): Set<UUID> {
        return items.flatMap(transformation).toSet()
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authz
    }

    override fun getAuditingManager(): AuditingManager {
        return auditingManager
    }
}