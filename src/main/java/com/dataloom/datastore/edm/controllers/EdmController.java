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

package com.dataloom.datastore.edm.controllers;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.auth0.jwt.internal.org.apache.commons.lang3.StringUtils;
import com.auth0.spring.security.api.Auth0JWTToken;
import com.dataloom.authentication.LoomAuth0AuthenticationProvider;
import com.dataloom.authorization.AbstractSecurableObjectResolveTypeService;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.AuthorizingComponent;
import com.dataloom.authorization.ForbiddenException;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principals;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.authorization.util.AuthorizationUtils;
import com.dataloom.datastore.constants.CustomMediaType;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.EdmDetails;
import com.dataloom.edm.EntityDataModel;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.Schema;
import com.dataloom.edm.requests.EdmDetailsSelector;
import com.dataloom.edm.requests.EdmRequest;
import com.dataloom.edm.requests.MetadataUpdate;
import com.dataloom.edm.schemas.manager.HazelcastSchemaManager;
import com.dataloom.edm.type.ComplexType;
import com.dataloom.edm.type.EdgeType;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.EnumType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.exceptions.ErrorsDTO;
import com.dataloom.exceptions.LoomExceptions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.kryptnostic.datastore.exceptions.BadRequestException;
import com.kryptnostic.datastore.exceptions.BatchException;
import com.kryptnostic.datastore.services.CassandraDataManager;
import com.kryptnostic.datastore.services.CassandraEntitySetManager;
import com.kryptnostic.datastore.services.DatasourceManager;
import com.kryptnostic.datastore.services.EdmManager;

@RestController
@RequestMapping( EdmApi.CONTROLLER )
public class EdmController implements EdmApi, AuthorizingComponent {

    @Inject
    private EdmManager                                modelService;

    @Inject
    private HazelcastSchemaManager                    schemaManager;

    @Inject
    private AuthorizationManager                      authorizations;

    @Inject
    private CassandraEntitySetManager                 entitySetManager;

    @Inject
    private AbstractSecurableObjectResolveTypeService securableObjectTypes;

    @Inject
    private LoomAuth0AuthenticationProvider           authProvider;

    @Inject
    private CassandraDataManager                      dataManager;

    @Inject
    private DatasourceManager                         datasourceManager;

    @Override
    @RequestMapping(
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public EntityDataModel getEntityDataModel() {
        final Iterable<Schema> schemas = schemaManager.getAllSchemas();
        final Iterable<EntityType> entityTypes = getEntityTypes();
        final Iterable<PropertyType> propertyTypes = getPropertyTypes();
        final Set<String> namespaces = Sets.newHashSet();
        getEntityTypes().forEach( entityType -> namespaces.add( entityType.getType().getNamespace() ) );
        getPropertyTypes().forEach( propertyType -> namespaces.add( propertyType.getType().getNamespace() ) );

        Iterable<EntitySet> authorizedEntitySets = getAccessibleObjects( SecurableObjectType.EntitySet,
                EnumSet.of( Permission.READ ) )
                        .map( AuthorizationUtils::getLastAclKeySafely )
                        .map( modelService::getEntitySet )::iterator;

        return new EntityDataModel(
                namespaces,
                schemas,
                entityTypes,
                propertyTypes,
                authorizedEntitySets::iterator );
    }

    @Override
    @RequestMapping(
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public EdmDetails getEdmDetails( @RequestBody Set<EdmDetailsSelector> selectors ) {
        final Set<UUID> propertyTypeIds = new HashSet<>();
        final Set<UUID> entityTypeIds = new HashSet<>();
        final Set<UUID> entitySetIds = new HashSet<>();

        selectors.forEach( selector -> {
            switch ( selector.getType() ) {
                case PropertyTypeInEntitySet:
                    updatePropertyTypeIdsToGet( selector, propertyTypeIds );
                    break;
                case EntityType:
                    updateEntityTypeIdsToGet( selector, propertyTypeIds, entityTypeIds );
                    break;
                case EntitySet:
                    updateEntitySetIdsToGet( selector, propertyTypeIds, entityTypeIds, entitySetIds );
                    break;
                default:
                    throw new BadRequestException(
                            "Unsupported Securable Object Type when retrieving Edm Details: " + selector.getType() );
            }
        } );
        return new EdmDetails(
                modelService.getPropertyTypesAsMap( propertyTypeIds ),
                modelService.getEntityTypesAsMap( entityTypeIds ),
                modelService.getEntitySetsAsMap( entitySetIds ) );
    }

    private void updatePropertyTypeIdsToGet( EdmDetailsSelector selector, Set<UUID> propertyTypeIds ) {
        if ( selector.getIncludedFields().contains( SecurableObjectType.PropertyTypeInEntitySet ) ) {
            propertyTypeIds.add( selector.getId() );
        }
    }

    private void updateEntityTypeIdsToGet(
            EdmDetailsSelector selector,
            Set<UUID> propertyTypeIds,
            Set<UUID> entityTypeIds ) {
        if ( selector.getIncludedFields().contains( SecurableObjectType.EntityType ) ) {
            entityTypeIds.add( selector.getId() );
        }
        if ( selector.getIncludedFields().contains( SecurableObjectType.PropertyTypeInEntitySet ) ) {
            EntityType et = modelService.getEntityType( selector.getId() );
            if ( et != null ) {
                propertyTypeIds.addAll( et.getProperties() );
            }
        }
    }

    private void updateEntitySetIdsToGet(
            EdmDetailsSelector selector,
            Set<UUID> propertyTypeIds,
            Set<UUID> entityTypeIds,
            Set<UUID> entitySetIds ) {
        boolean setRetrieved = false;
        EntitySet es = null;
        if ( selector.getIncludedFields().contains( SecurableObjectType.EntitySet ) ) {
            entitySetIds.add( selector.getId() );
        }
        if ( selector.getIncludedFields().contains( SecurableObjectType.EntityType ) ) {
            es = modelService.getEntitySet( selector.getId() );
            setRetrieved = true;
            if ( es != null ) {
                entityTypeIds.add( es.getEntityTypeId() );
            }
        }
        if ( selector.getIncludedFields().contains( SecurableObjectType.PropertyTypeInEntitySet ) ) {
            if ( !setRetrieved ) {
                es = modelService.getEntitySet( selector.getId() );
            }
            if ( es != null ) {
                EntityType et = modelService.getEntityType( es.getEntityTypeId() );
                if ( et != null ) {
                    propertyTypeIds.addAll( et.getProperties() );
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see com.kryptnostic.datastore.edm.controllers.EdmAPI#getSchemas()
     */
    @Override
    @RequestMapping(
        path = SCHEMA_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Schema> getSchemas() {
        return schemaManager.getAllSchemas();
    }

    @Override
    @RequestMapping(
        path = SCHEMA_PATH + NAMESPACE_PATH + NAME_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Schema getSchemaContents(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name ) {
        return schemaManager.getSchema( namespace, name );
    }

    private static void setDownloadContentType( HttpServletResponse response, FileType fileType ) {
        if ( fileType == FileType.json ) {
            response.setContentType( MediaType.APPLICATION_JSON_VALUE );
        } else {
            response.setContentType( CustomMediaType.TEXT_YAML_VALUE );
        }
    }

    private static void setContentDisposition(
            HttpServletResponse response,
            String fileName,
            FileType fileType ) {
        if ( fileType == FileType.yaml || fileType == FileType.json ) {
            response.setHeader( "Content-Disposition",
                    "attachment; filename=" + fileName + "." + fileType.toString() );
        }
    }

    @RequestMapping(
        path = SCHEMA_PATH + NAMESPACE_PATH + NAME_PATH,
        method = RequestMethod.GET,
        produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_YAML_VALUE } )
    public Schema getSchemaContentsFormatted(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @RequestParam(
                value = FILE_TYPE,
                required = true ) FileType fileType,
            @RequestParam(
                value = TOKEN,
                required = false ) String token,
            HttpServletResponse response ) {
        setContentDisposition( response, namespace + "." + name, fileType );
        setDownloadContentType( response, fileType );

        return getSchemaContentsFormatted( namespace, name, fileType, token );
    }

    @Override
    public Schema getSchemaContentsFormatted(
            String namespace,
            String name,
            FileType fileType,
            String token ) {
        if ( StringUtils.isNotBlank( token ) ) {
            Authentication authentication = authProvider.authenticate( new Auth0JWTToken( token ) );
            SecurityContextHolder.getContext().setAuthentication( authentication );
        }
        return schemaManager.getSchema( namespace, name );
    }

    @Override
    @RequestMapping(
        path = SCHEMA_PATH + NAMESPACE_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Schema> getSchemasInNamespace( @PathVariable( NAMESPACE ) String namespace ) {
        return schemaManager.getSchemasInNamespace( namespace );
    }

    @Override
    @RequestMapping(
        path = SCHEMA_PATH,
        method = RequestMethod.POST )
    @ResponseStatus( HttpStatus.OK )
    public Void createSchemaIfNotExists( @RequestBody Schema schema ) {
        schemaManager.createOrUpdateSchemas( schema );
        return null;
    }

    @Override
    @RequestMapping(
        path = SCHEMA_PATH + NAMESPACE_PATH + NAME_PATH,
        method = RequestMethod.PUT )
    @ResponseStatus( HttpStatus.OK )
    public Void createEmptySchema( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        schemaManager.upsertSchemas( ImmutableSet.of( new FullQualifiedName( namespace, name ) ) );
        return null;
    }

    @Override
    @RequestMapping(
        path = ENTITY_SETS_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, UUID> createEntitySets( @RequestBody Set<EntitySet> entitySets ) {
        ErrorsDTO dto = new ErrorsDTO();

        Map<String, UUID> createdEntitySets = Maps.newHashMapWithExpectedSize( entitySets.size() );
        // TODO: Add access check to make sure user can create entity sets.
        for ( EntitySet entitySet : entitySets ) {
            try {
                ensureValidEntitySet( entitySet );
                modelService.createEntitySet( Principals.getCurrentUser(), entitySet );
                createdEntitySets.put( entitySet.getName(), entitySet.getId() );
                securableObjectTypes.createSecurableObjectType( ImmutableList.of( entitySet.getId() ),
                        SecurableObjectType.EntitySet );
                datasourceManager.setCurrentSyncId( entitySet.getId(),
                        datasourceManager.createNewSyncIdForEntitySet( entitySet.getId() ) );
            } catch ( Exception e ) {
                dto.addError( LoomExceptions.OTHER_EXCEPTION, entitySet.getName() + ": " + e.getMessage() );
            }
        }

        if ( !dto.isEmpty() ) {
            throw new BatchException( dto );
        }
        return createdEntitySets;
    }

    @Override
    @RequestMapping(
        path = ENTITY_SETS_PATH,
        method = RequestMethod.GET )
    public Iterable<EntitySet> getEntitySets() {
        return authorizations.getAuthorizedObjectsOfType(
                Principals.getCurrentPrincipals(),
                SecurableObjectType.EntitySet,
                EnumSet.of( Permission.READ ) )
                .map( AuthorizationUtils::getLastAclKeySafely )
                .map( modelService::getEntitySet )::iterator;
    }

    @Override
    @RequestMapping(
        path = ENTITY_SETS_PATH + ID_PATH,
        method = RequestMethod.GET )
    public EntitySet getEntitySet( @PathVariable( ID ) UUID entitySetId ) {
        if ( isAuthorized( Permission.READ ).test( ImmutableList.of( entitySetId ) ) ) {
            return modelService.getEntitySet( entitySetId );
        } else {
            throw new ForbiddenException( "Unable to find entity set: " + entitySetId );
        }
    }

    @Override
    @RequestMapping(
        path = ENTITY_SETS_PATH + ID_PATH,
        method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void deleteEntitySet( @PathVariable( ID ) UUID entitySetId ) {
        ensureOwnerAccess( Arrays.asList( entitySetId ) );
        modelService.deleteEntitySet( entitySetId );
        securableObjectTypes.deleteSecurableObjectType( ImmutableList.of( entitySetId ) );

        dataManager.deleteEntitySetData( entitySetId );

        return null;
    }

    @Override
    @RequestMapping(
        path = SCHEMA_PATH + NAMESPACE_PATH + NAME_PATH,
        method = RequestMethod.PATCH,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void updateSchema(
            @PathVariable( NAMESPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @RequestBody EdmRequest request ) {
        final Set<UUID> propertyTypes = request.getPropertyTypes();
        final Set<UUID> entityTypes = request.getEntityTypes();
        final FullQualifiedName schemaName = new FullQualifiedName( namespace, name );
        switch ( request.getAction() ) {
            case ADD:
                schemaManager.addEntityTypesToSchema( entityTypes, schemaName );
                schemaManager.addPropertyTypesToSchema( propertyTypes, schemaName );
                break;
            case REMOVE:
                schemaManager.removeEntityTypesFromSchema( entityTypes, schemaName );
                schemaManager.removePropertyTypesFromSchema( propertyTypes, schemaName );
                break;
            case REPLACE:
                final Set<UUID> existingPropertyTypes = schemaManager.getAllPropertyTypesInSchema( schemaName );
                final Set<UUID> existingEntityTypes = schemaManager.getAllEntityTypesInSchema( schemaName );

                final Set<UUID> propertyTypesToAdd = Sets.difference( propertyTypes, existingPropertyTypes );
                final Set<UUID> propertyTypesToRemove = Sets.difference( existingPropertyTypes, propertyTypes );
                schemaManager.removePropertyTypesFromSchema( propertyTypesToRemove, schemaName );
                schemaManager.addPropertyTypesToSchema( propertyTypesToAdd, schemaName );

                final Set<UUID> entityTypesToAdd = Sets.difference( entityTypes, existingEntityTypes );
                final Set<UUID> entityTypesToRemove = Sets.difference( existingEntityTypes, entityTypes );
                schemaManager.removeEntityTypesFromSchema( entityTypesToAdd, schemaName );
                schemaManager.addEntityTypesToSchema( entityTypesToRemove, schemaName );
                break;
        }
        return null;
    }

    @Override
    @PostMapping(
        path = ENUM_TYPE_PATH,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public UUID createEnumType( @RequestBody EnumType enumType ) {
        modelService.createEnumTypeIfNotExists( enumType );
        return enumType.getId();
    }

    @Override
    @GetMapping(
        path = ENUM_TYPE_PATH,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<EnumType> getEnumTypes() {
        return modelService.getEnumTypes()::iterator;
    }

    @Override
    @GetMapping(
        path = ENUM_TYPE_PATH + ID_PATH,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public EnumType getEnumType( @PathVariable( ID ) UUID enumTypeId ) {
        return modelService.getEnumType( enumTypeId );
    }

    @Override
    @GetMapping(
        path = ENTITY_TYPE_PATH + ID_PATH + HIERARCHY_PATH,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Set<EntityType> getEntityTypeHierarchy( @PathVariable( ID ) UUID entityTypeId ) {
        return modelService.getEntityTypeHierarchy( entityTypeId );
    }

    @Override
    @DeleteMapping(
        path = ENUM_TYPE_PATH + ID_PATH )
    public Void deleteEnumType( @PathVariable( ID ) UUID enumTypeId ) {
        modelService.deleteEnumType( enumTypeId );
        return null;
    }

    @Override
    @GetMapping(
        path = COMPLEX_TYPE_PATH,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<ComplexType> getComplexTypes() {
        return modelService.getComplexTypes()::iterator;
    }

    @Override
    @PostMapping(
        path = COMPLEX_TYPE_PATH,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public UUID createComplexType( @RequestBody ComplexType complexType ) {
        modelService.createComplexTypeIfNotExists( complexType );
        return complexType.getId();
    }

    @Override
    @GetMapping(
        path = COMPLEX_TYPE_PATH + ID_PATH,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public ComplexType getComplexType( @PathVariable( ID ) UUID complexTypeId ) {
        return modelService.getComplexType( complexTypeId );
    }

    @Override
    @GetMapping(
        path = COMPLEX_TYPE_PATH + ID_PATH + HIERARCHY_PATH,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Set<ComplexType> getComplexTypeHierarchy( @PathVariable( ID ) UUID complexTypeId ) {
        return modelService.getComplexTypeHierarchy( complexTypeId );
    }

    @Override
    @DeleteMapping(
        path = COMPLEX_TYPE_PATH + ID_PATH,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Void deleteComplexType( @PathVariable( ID ) UUID complexTypeId ) {
        modelService.deleteComplexType( complexTypeId );
        return null;
    }

    @Override
    @RequestMapping(
        path = ENTITY_TYPE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public UUID createEntityType( @RequestBody EntityType entityType ) {
        ensureValidEntityType( entityType );
        modelService.createEntityType( entityType );
        return entityType.getId();
    }

    @Override
    @RequestMapping(
        path = ENTITY_TYPE_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<EntityType> getEntityTypes() {
        return modelService.getEntityTypes()::iterator;
    }

    @Override
    @RequestMapping(
        path = ENTITY_TYPE_PATH + ID_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public EntityType getEntityType( @PathVariable( ID ) UUID entityTypeId ) {
        return Preconditions.checkNotNull( modelService.getEntityType( entityTypeId ),
                "Unable to find entity type: " + entityTypeId );
    }

    @Override
    @RequestMapping(
        path = ENTITY_TYPE_PATH + ENTITY_TYPE_ID_PATH + PROPERTY_TYPE_ID_PATH,
        method = RequestMethod.PUT )
    @ResponseStatus( HttpStatus.OK )
    public Void addPropertyTypeToEntityType(
            @PathVariable( ENTITY_TYPE_ID ) UUID entityTypeId,
            @PathVariable( PROPERTY_TYPE_ID ) UUID propertyTypeId ) {
        ensureAdminAccess();
        modelService.addPropertyTypesToEntityType( entityTypeId, ImmutableSet.of( propertyTypeId ) );
        return null;
    }

    @Override
    @RequestMapping(
        path = ENTITY_TYPE_PATH + ENTITY_TYPE_ID_PATH + PROPERTY_TYPE_ID_PATH,
        method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void removePropertyTypeFromEntityType(
            @PathVariable( ENTITY_TYPE_ID ) UUID entityTypeId,
            @PathVariable( PROPERTY_TYPE_ID ) UUID propertyTypeId ) {
        ensureAdminAccess();
        modelService.removePropertyTypesFromEntityType( entityTypeId, ImmutableSet.of( propertyTypeId ) );
        return null;
    }

    @Override
    @RequestMapping(
        path = ENTITY_TYPE_PATH + ID_PATH,
        method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void deleteEntityType( @PathVariable( ID ) UUID entityTypeId ) {
        ensureAdminAccess();
        modelService.deleteEntityType( entityTypeId );
        return null;
    }

    @Override
    @RequestMapping(
        path = PROPERTY_TYPE_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PropertyType> getPropertyTypes() {
        return modelService.getPropertyTypes()::iterator;
    }

    @Override
    @RequestMapping(
        path = PROPERTY_TYPE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public UUID createPropertyType( @RequestBody PropertyType propertyType ) {
        modelService.createPropertyTypeIfNotExists( propertyType );
        return propertyType.getId();
    }

    @Override
    @RequestMapping(
        path = PROPERTY_TYPE_PATH + ID_PATH,
        method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void deletePropertyType(
            @PathVariable( ID ) UUID propertyTypeId ) {
        ensureAdminAccess();
        modelService.deletePropertyType( propertyTypeId );
        return null;
    }

    @Override
    @RequestMapping(
        path = PROPERTY_TYPE_PATH + ID_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public PropertyType getPropertyType( @PathVariable( ID ) UUID propertyTypeId ) {
        return modelService.getPropertyType( propertyTypeId );
    }

    @Override
    @RequestMapping(
        path = NAMESPACE + NAMESPACE_PATH + PROPERTY_TYPE_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<PropertyType> getPropertyTypesInNamespace( @PathVariable( NAMESPACE ) String namespace ) {
        return modelService.getPropertyTypesInNamespace( namespace );
    }

    @Override
    @RequestMapping(
        path = IDS_PATH + ENTITY_SETS_PATH + NAME_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public UUID getEntitySetId( @PathVariable( NAME ) String entitySetName ) {
        EntitySet es = entitySetManager.getEntitySet( entitySetName );
        Preconditions.checkNotNull( es, "Entity Set %s does not exist.", entitySetName );
        return es.getId();
    }

    @Override
    @RequestMapping(
        path = IDS_PATH + PROPERTY_TYPE_PATH + NAMESPACE_PATH + NAME_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public UUID getPropertyTypeId( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        FullQualifiedName fqn = new FullQualifiedName( namespace, name );
        return Preconditions.checkNotNull( modelService.getTypeAclKey( fqn ),
                "Property Type %s does not exist.",
                fqn.getFullQualifiedNameAsString() );
    }

    @Override
    @RequestMapping(
        path = IDS_PATH + ENTITY_TYPE_PATH + NAMESPACE_PATH + NAME_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public UUID getEntityTypeId( @PathVariable( NAMESPACE ) String namespace, @PathVariable( NAME ) String name ) {
        FullQualifiedName fqn = new FullQualifiedName( namespace, name );
        return Preconditions.checkNotNull( modelService.getTypeAclKey( fqn ),
                "Entity Type %s does not exist.",
                fqn.getFullQualifiedNameAsString() );
    }

    @Override
    @RequestMapping(
        path = PROPERTY_TYPE_PATH + ID_PATH,
        method = RequestMethod.PATCH,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void updatePropertyTypeMetadata(
            @PathVariable( ID ) UUID propertyTypeId,
            @RequestBody MetadataUpdate update ) {
        ensureAdminAccess();
        modelService.updatePropertyTypeMetadata( propertyTypeId, update );
        return null;
    }

    @Override
    @RequestMapping(
        path = ENTITY_TYPE_PATH + ID_PATH,
        method = RequestMethod.PATCH,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void updateEntityTypeMetadata( @PathVariable( ID ) UUID entityTypeId, @RequestBody MetadataUpdate update ) {
        ensureAdminAccess();
        modelService.updateEntityTypeMetadata( entityTypeId, update );
        return null;
    }

    @Override
    @RequestMapping(
        path = ENTITY_SETS_PATH + ID_PATH,
        method = RequestMethod.PATCH,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    public Void updateEntitySetMetadata( @PathVariable( ID ) UUID entitySetId, @RequestBody MetadataUpdate update ) {
        ensureAdminAccess();
        modelService.updateEntitySetMetadata( entitySetId, update );
        return null;
    }

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

    private void ensureValidEntityType( EntityType entityType ) {
        Preconditions.checkArgument( modelService.checkPropertyTypesExist( entityType.getProperties() ),
                "Some properties do not exist" );
    }

    private void ensureValidEntitySet( EntitySet entitySet ) {
        Preconditions.checkArgument( modelService.checkEntityTypeExists( entitySet.getEntityTypeId() ),
                "Entity Set Type does not exist." );
    }

    @Override
    @RequestMapping(
        path = EDGE_TYPE_PATH,
        method = RequestMethod.POST,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public UUID createEdgeType( @RequestBody EdgeType edgeType ) {
        EntityType entityType = edgeType.getEdgeEntityType();
        if ( entityType == null ) {
            throw new IllegalArgumentException( "You cannot create an edge type without specifying its entity type" );
        }
        createEntityType( entityType );
        modelService.createEdgeType( edgeType, entityType.getId() );
        return entityType.getId();
    }

    @Override
    @RequestMapping(
        path = EDGE_TYPE_PATH + ID_PATH,
        method = RequestMethod.DELETE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Void deleteEdgeType( @PathVariable( ID ) UUID edgeTypeId ) {
        ensureAdminAccess();
        modelService.deleteEdgeType( edgeTypeId );
        return null;
    }

    @Override
    @RequestMapping(
        path = EDGE_TYPE_PATH + ID_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public EdgeType getEdgeTypeById( @PathVariable( ID ) UUID edgeTypeId ) {
        return modelService.getEdgeType( edgeTypeId );
    }
}
