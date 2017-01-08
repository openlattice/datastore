package com.kryptnostic.datastore.data.controllers;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.BadRequestException;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpServerErrorException;

import com.dataloom.authorization.AclKeyPathFragment;
import com.dataloom.authorization.AuthorizationManager;
import com.dataloom.authorization.EdmAuthorizationHelper;
import com.dataloom.authorization.Principals;
import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.authorization.requests.Permission;
import com.dataloom.data.DataApi;
import com.dataloom.data.requests.CreateEntityRequest;
import com.dataloom.data.requests.LookupEntitiesRequest;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.kryptnostic.datastore.constants.CustomMediaType;
import com.kryptnostic.datastore.constants.DatastoreConstants;
import com.kryptnostic.datastore.exceptions.ResourceNotFoundException;
import com.kryptnostic.datastore.services.DataService;
import com.kryptnostic.datastore.services.EdmManager;

@RestController
@RequestMapping( "/" + DataApi.CONTROLLER )
public class DataController implements DataApi {
    private static final Logger logger = LoggerFactory.getLogger( DataController.class );

    // TODO: Move to DataApi
    // TODO: Make spring handling case insensitive to follow correct Java style.
    public static enum FileType {
        json,
        csv;
    }

    @Inject
    private EdmManager                 dms;

    @Inject
    private DataService                dataService;

    @Inject
    private AuthorizationManager       authz;

    @Inject
    private EdmAuthorizationHelper     authzHelper;

    @RequestMapping(
        path = { "/object/{id}" },
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, Object> getObject( @PathVariable( "id" ) UUID objectId ) {
        return dataService.getObject( objectId );
    }

    @RequestMapping(
        path = "/" + DataApi.ENTITY_DATA + "/" + DataApi.NAME_SPACE_PATH + "/" + DataApi.NAME_PATH + "/"
                + DataApi.SET_NAME_PATH,
        method = RequestMethod.GET,
        produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfEntitySet(
            @PathVariable( SET_NAME ) String entitySetName,
            @PathVariable( NAME_SPACE ) String entityTypeNamespace,
            @PathVariable( NAME ) String entityTypeName,
            @RequestParam(
                value = DatastoreConstants.FILE_TYPE,
                required = false ) FileType fileType,
            HttpServletResponse response ) {
        String fileName = entitySetName;
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return getAllEntitiesOfEntitySet( entitySetName, entityTypeNamespace, entityTypeName );
    }

    @Override
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfEntitySet(
            String entitySetName,
            String entityTypeNamespace,
            String entityTypeName ) {

        if ( authzService.getAllEntitiesOfEntitySet( entitySetName ) ) {
            EntityType entityType = dms.getEntityType( entityTypeNamespace, entityTypeName );
            Set<FullQualifiedName> authorizedPropertyFqns = entityType.getProperties().stream()
                    .filter( propertyTypeFqn -> authzService.readPropertyTypeInEntitySet( entitySetName,
                            propertyTypeFqn ) )
                    .collect( Collectors.toSet() );
            return dataService.getAllEntitiesOfEntitySet( entitySetName,
                    entityTypeNamespace,
                    entityTypeName,
                    authorizedPropertyFqns );
        }
        return null;
    }

    @RequestMapping(
        path = "/" + DataApi.ENTITY_DATA + "/" + DataApi.NAME_SPACE_PATH + "/" + DataApi.NAME_PATH + "/"
                + DataApi.SET_NAME_PATH + "/" + DataApi.SELECTED,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Multimap<FullQualifiedName, Object>> getSelectedEntitiesOfEntitySet(
            @PathVariable( SET_NAME ) String entitySetName,
            @PathVariable( NAME_SPACE ) String entityTypeNamespace,
            @PathVariable( NAME ) String entityTypeName,
            @RequestParam(
                value = DatastoreConstants.FILE_TYPE,
                required = false ) FileType fileType,
            @RequestBody Set<FullQualifiedName> selectedProperties,
            HttpServletResponse response ) {
        String fileName = entitySetName + "_selected";
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return getSelectedEntitiesOfEntitySet( entitySetName, entityTypeNamespace, entityTypeName, selectedProperties );
    }

    @Override
    public Iterable<Multimap<FullQualifiedName, Object>> getSelectedEntitiesOfEntitySet(
            String entitySetName,
            String entityTypeNamespace,
            String entityTypeName,
            Set<FullQualifiedName> selectedProperties ) {

        if ( authzService.getAllEntitiesOfEntitySet( entitySetName ) ) {
            EntityType entityType = dms.getEntityType( entityTypeNamespace, entityTypeName );
            Set<FullQualifiedName> targetPropertyFqns = Sets.intersection( entityType.getProperties(),
                    selectedProperties );
            Set<FullQualifiedName> authorizedPropertyFqns = targetPropertyFqns.stream()
                    .filter( propertyTypeFqn -> authzService.readPropertyTypeInEntitySet( entitySetName,
                            propertyTypeFqn ) )
                    .collect( Collectors.toSet() );
            return dataService.getAllEntitiesOfEntitySet( entitySetName,
                    entityTypeNamespace,
                    entityTypeName,
                    authorizedPropertyFqns );
        }
        return null;
    }

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
        path = "/" + DataApi.ENTITY_DATA,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType(
            @RequestBody FullQualifiedName fqn,
            @RequestParam(
                value = DatastoreConstants.FILE_TYPE,
                required = false ) FileType fileType,
            HttpServletResponse response ) {
        String fileName = fqn.getNamespace() + "_" + fqn.getName();
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return getAllEntitiesOfType( fqn );
    }

    @Override
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType(
            FullQualifiedName fqn ) {
        if ( dms.checkEntityTypeExists( fqn ) && authzService.getAllEntitiesOfType( fqn ) ) {
            EntityType entityType = dms.getEntityType( fqn );
            Set<FullQualifiedName> authorizedPropertyFqns = entityType.getProperties().stream()
                    .filter( propertyTypeFqn -> authzService.readPropertyTypeInEntityType( fqn, propertyTypeFqn ) )
                    .collect( Collectors.toSet() );
            return dataService.readAllEntitiesOfType( fqn, authorizedPropertyFqns );
        } else {
            throw new ResourceNotFoundException( "Entity Type not found." );
        }
    }

    @RequestMapping(
        path = "/" + DataApi.ENTITY_DATA + "/" + DataApi.FULLQUALIFIEDNAME_PATH_WITH_DOT,
        method = RequestMethod.GET,
        produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType(
            @PathVariable( FULLQUALIFIEDNAME ) String fqnAsString,
            @RequestParam(
                value = DatastoreConstants.FILE_TYPE,
                required = false ) FileType fileType,
            HttpServletResponse response ) {
        FullQualifiedName fqn = new FullQualifiedName( fqnAsString );
        String fileName = fqn.getNamespace() + "_" + fqn.getName();
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return getAllEntitiesOfType( fqn );
    }

    @Override
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType( String fqnAsString ) {
        return getAllEntitiesOfType( new FullQualifiedName( fqnAsString ) );
    }

    @RequestMapping(
        path = "/" + DataApi.ENTITY_DATA + "/" + DataApi.NAME_SPACE_PATH + "/" + DataApi.NAME_PATH,
        method = RequestMethod.GET,
        produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType(
            @PathVariable( NAME_SPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @RequestParam(
                value = DatastoreConstants.FILE_TYPE,
                required = false ) FileType fileType,
            HttpServletResponse response ) {
        String fileName = namespace + "_" + name;
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return getAllEntitiesOfType( new FullQualifiedName( namespace, name ) );
    }

    @Override
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType( String namespace, String name ) {
        return getAllEntitiesOfType( new FullQualifiedName( namespace, name ) );
    }

    @RequestMapping(
        path = "/" + DataApi.ENTITY_DATA + "/" + DataApi.NAME_SPACE_PATH + "/" + DataApi.NAME_PATH + "/"
                + DataApi.SELECTED,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Multimap<FullQualifiedName, Object>> getSelectedEntitiesOfType(
            @PathVariable( NAME_SPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @RequestParam(
                value = DatastoreConstants.FILE_TYPE,
                required = false ) FileType fileType,
            HttpServletResponse response,
            @RequestBody Set<FullQualifiedName> selectedProperties ) {
        String fileName = namespace + "_" + name + "_selected";
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return getSelectedEntitiesOfType( namespace, name, selectedProperties );
    }

    @Override
    public Iterable<Multimap<FullQualifiedName, Object>> getSelectedEntitiesOfType(
            String namespace,
            String name,
            Set<FullQualifiedName> selectedProperties ) {
        FullQualifiedName fqn = new FullQualifiedName( namespace, name );
        if ( dms.checkEntityTypeExists( fqn ) && authzService.getAllEntitiesOfType( fqn ) ) {
            EntityType entityType = dms.getEntityType( fqn );
            Set<FullQualifiedName> targetPropertyFqns = Sets.intersection( entityType.getProperties(),
                    selectedProperties );

            Set<FullQualifiedName> authorizedPropertyFqns = targetPropertyFqns.stream()
                    .filter( propertyTypeFqn -> authzService.readPropertyTypeInEntityType( fqn, propertyTypeFqn ) )
                    .collect( Collectors.toSet() );

            return dataService.readAllEntitiesOfType( fqn, authorizedPropertyFqns );
        } else {
            throw new ResourceNotFoundException( "Entity Type not found." );
        }
    }

    @RequestMapping(
        path = "/" + DataApi.ENTITY_DATA + "/" + DataApi.MULTIPLE,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Iterable<Multimap<FullQualifiedName, Object>>> getAllEntitiesOfTypes(
            @RequestBody List<FullQualifiedName> fqns,
            @RequestParam(
                value = DatastoreConstants.FILE_TYPE,
                required = false ) FileType fileType,
            HttpServletResponse response ) {
        if ( fileType == FileType.csv ) {
            throw new BadRequestException( "csv format file is supported for this endpoint." );
        }
        setContentDisposition( response, "entities_data", fileType );
        return getAllEntitiesOfTypes( fqns );
    }

    @Override
    public Iterable<Iterable<Multimap<FullQualifiedName, Object>>> getAllEntitiesOfTypes(
            List<FullQualifiedName> fqns ) {
        Map<FullQualifiedName, Collection<FullQualifiedName>> entityTypesAndAuthorizedProperties = new HashMap<>();

        for ( FullQualifiedName fqn : fqns ) {
            EntityType entityType = dms.getEntityType( fqn );
            if ( dms.checkEntityTypeExists( fqn ) && authzService.getAllEntitiesOfType( fqn ) ) {
                Set<FullQualifiedName> authorizedPropertyFqns = entityType.getProperties().stream()
                        .filter( propertyTypeFqn -> authzService.readPropertyTypeInEntityType( fqn, propertyTypeFqn ) )
                        .collect( Collectors.toSet() );
                entityTypesAndAuthorizedProperties.put( fqn, authorizedPropertyFqns );
            } else {
                logger.error( "GetAllEntitiesOfType for " + fqn + " failed for user " + authzService.getUserId() );
            }
        }
        return dataService.readAllEntitiesOfSchema( entityTypesAndAuthorizedProperties );
    }

    @Override
    @RequestMapping(
        path = "/" + DataApi.ENTITY_DATA + "/" + DataApi.FILTERED,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<UUID> getFilteredEntities( @RequestBody LookupEntitiesRequest lookupEntitiesRequest ) {
        return dataService.getFilteredEntities( lookupEntitiesRequest );
    }

    @Override
    @RequestMapping(
        path = "/" + DataApi.ENTITY_DATA,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void createEntityData( @RequestBody CreateEntityRequest createEntityRequest ) {
        EntitySet entitySet = dms.getEntitySet( createEntityRequest.getEntitySetName() );
        AclKeyPathFragment entitySetAclKey = new AclKeyPathFragment( SecurableObjectType.EntitySet, entitySet.getId() );

        if ( authz.checkIfHasPermissions( Arrays.asList( entitySetAclKey ),
                Principals.getCurrentPrincipals(),
                EnumSet.of( Permission.WRITE ) ) ) {
            Set<FullQualifiedName> authorizedPropertyTypes = authzHelper.getAuthorizedPropertiesOnEntitySet( entitySet,
                    EnumSet.of( Permission.WRITE ) );
            dataService.createEntityData( createEntityRequest, authorizedPropertyTypes );
        } else {
            throw new HttpServerErrorException(
                    HttpStatus.FORBIDDEN,
                    "Insufficient permissions to write to entity set or it doesn't exist" );
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + DataApi.INTEGRATION,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, String> getAllIntegrationScripts() {
        return dataService.getAllIntegrationScripts();
    }

    @Override
    @RequestMapping(
        path = "/" + DataApi.INTEGRATION,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, String> getIntegrationScript( @RequestBody Set<String> urls ) {
        return dataService.getIntegrationScriptForUrl( urls );
    }

    @Override
    @RequestMapping(
        path = "/" + DataApi.INTEGRATION,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void createIntegrationScript( @RequestBody Map<String, String> integrationScripts ) {
        dataService.createIntegrationScript( integrationScripts );
        return null;
    }

}
