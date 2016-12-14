package com.kryptnostic.datastore.data.controllers;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
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

import com.dataloom.data.DataApi;
import com.dataloom.data.requests.CreateEntityRequest;
import com.dataloom.data.requests.LookupEntitiesRequest;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.validation.ValidateFullQualifiedName;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.kryptnostic.datastore.constants.CustomMediaType;
import com.kryptnostic.datastore.constants.DatastoreConstants;
import com.kryptnostic.datastore.exceptions.BatchExceptions;
import com.kryptnostic.datastore.exceptions.ForbiddenException;
import com.kryptnostic.datastore.services.ActionAuthorizationService;
import com.kryptnostic.datastore.services.DataService;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.util.ErrorsDTO;

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
    private ActionAuthorizationService authzService;

    @RequestMapping(
        path = { "/object/{id}" },
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, Object> getObject( @PathVariable( "id" ) UUID objectId ) {
        return dataService.getObject( objectId );
    }

    @RequestMapping(
        path = "/" + DataApi.ENTITY_DATA + "/" + DataApi.NAME_SPACE_PATH + "/" + DataApi.NAME_PATH + "/" + DataApi.SET_NAME_PATH,
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
            dms.ensureEntitySetExists( entityType.getTypename(), entitySetName );
            
            Set<FullQualifiedName> authorizedPropertyFqns = entityType.getProperties().stream()
                    .filter( propertyTypeFqn -> authzService.readPropertyTypeInEntitySet( entitySetName,
                            propertyTypeFqn ) )
                    .collect( Collectors.toSet() );
            return dataService.getAllEntitiesOfEntitySet( entitySetName,
                    entityTypeNamespace,
                    entityTypeName,
                    authorizedPropertyFqns );
        } else {
            throw new ForbiddenException();
        }
    }

    @RequestMapping(
            path = "/" + DataApi.ENTITY_DATA + "/" + DataApi.NAME_SPACE_PATH + "/" + DataApi.NAME_PATH + "/" + DataApi.SET_NAME_PATH + "/" + DataApi.SELECTED,
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
            @RequestBody @Valid Set<@ValidateFullQualifiedName FullQualifiedName> selectedProperties,
            HttpServletResponse response ) {
        // TODO Cascade validation not working.
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
            dms.ensureEntitySetExists( entityType.getTypename(), entitySetName );
            
            //This automatically ignores non-existing property types
            Set<FullQualifiedName> targetPropertyFqns = Sets.intersection( entityType.getProperties(), selectedProperties );
            Set<FullQualifiedName> authorizedPropertyFqns = targetPropertyFqns.stream()
                    .filter( propertyTypeFqn -> authzService.readPropertyTypeInEntitySet( entitySetName,
                            propertyTypeFqn ) )
                    .collect( Collectors.toSet() );
            return dataService.getAllEntitiesOfEntitySet( entitySetName,
                    entityTypeNamespace,
                    entityTypeName,
                    authorizedPropertyFqns );
        } else {
            throw new ForbiddenException();
        }
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
            @RequestBody @ValidateFullQualifiedName FullQualifiedName fqn,
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
        if( dms.checkEntityTypeExists( fqn ) && authzService.getAllEntitiesOfType( fqn ) ){
            EntityType entityType = dms.getEntityType( fqn );
            Set<FullQualifiedName> authorizedPropertyFqns = entityType.getProperties().stream()
                    .filter( propertyTypeFqn -> authzService.readPropertyTypeInEntityType( fqn, propertyTypeFqn ) )
                    .collect( Collectors.toSet() );
            return dataService.readAllEntitiesOfType( fqn, authorizedPropertyFqns );
        } else {
            throw new ForbiddenException();
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
            path = "/" + DataApi.ENTITY_DATA + "/" + DataApi.NAME_SPACE_PATH + "/" + DataApi.NAME_PATH + "/" + DataApi.SELECTED,
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
            @RequestBody @Valid Set<@ValidateFullQualifiedName FullQualifiedName> selectedProperties ) {
        // TODO Cascade validation not working.
        String fileName = namespace + "_" + name + "_selected";
        setContentDisposition( response, fileName, fileType );
        setDownloadContentType( response, fileType );
        return getSelectedEntitiesOfType( namespace, name, selectedProperties );
    }

    @Override
    public Iterable<Multimap<FullQualifiedName, Object>> getSelectedEntitiesOfType(
            String namespace, String name, Set<FullQualifiedName> selectedProperties ) {
        FullQualifiedName fqn = new FullQualifiedName( namespace, name );
        if( dms.checkEntityTypeExists( fqn ) && authzService.getAllEntitiesOfType( fqn ) ){
            EntityType entityType = dms.getEntityType( fqn );
            Set<FullQualifiedName> targetPropertyFqns = Sets.intersection( entityType.getProperties(), selectedProperties );

            Set<FullQualifiedName> authorizedPropertyFqns = targetPropertyFqns.stream()
                    .filter( propertyTypeFqn -> authzService.readPropertyTypeInEntityType( fqn, propertyTypeFqn ) )
                    .collect( Collectors.toSet() );

            return dataService.readAllEntitiesOfType( fqn, authorizedPropertyFqns );
        } else {
            throw new ForbiddenException();
        }
    }

    @RequestMapping(
        path = "/" + DataApi.ENTITY_DATA + "/" + DataApi.MULTIPLE,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Iterable<Multimap<FullQualifiedName, Object>>> getAllEntitiesOfTypes(
            @RequestBody @Valid List<@ValidateFullQualifiedName FullQualifiedName> fqns,
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
        
        ErrorsDTO dto = new ErrorsDTO();
        for( FullQualifiedName fqn : fqns ){
            EntityType entityType = dms.getEntityType( fqn );
            if( authzService.getAllEntitiesOfType( fqn ) ){
                Set<FullQualifiedName> authorizedPropertyFqns = entityType.getProperties().stream()
                        .filter( propertyTypeFqn -> authzService.readPropertyTypeInEntityType( fqn, propertyTypeFqn ) )
                        .collect( Collectors.toSet() );
                entityTypesAndAuthorizedProperties.put( fqn,  authorizedPropertyFqns );
            } else {
                logger.error( "GetAllEntitiesOfType for " + fqn + " failed for user " + authzService.getUserId() );
                dto.addError( ForbiddenException.class.getName(), fqn.toString(), ForbiddenException.message );
            }
        }
        
        if ( !dto.isEmpty() ) {
            throw new BatchExceptions( dto, HttpStatus.BAD_REQUEST );
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
    public Iterable<UUID> getFilteredEntities( @RequestBody @Valid LookupEntitiesRequest lookupEntitiesRequest ) {
        //TODO need to find a good way to validate keys in a map 
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
        boolean entitySetNamePresent = createEntityRequest.getEntitySetName().isPresent();
        boolean authorizedToWrite;

        if ( entitySetNamePresent ) {
            authorizedToWrite = authzService.createEntityOfEntitySet( createEntityRequest.getEntitySetName().get() );
        } else {
            authorizedToWrite = authzService.createEntityOfEntityType( createEntityRequest.getEntityType() );
        }
        
        if( authorizedToWrite ){
            EntityType entityType;
            
            entityType = dms.getEntityType( createEntityRequest.getEntityType() );
            
            Set<FullQualifiedName> authorizedPropertyFqns;

            if ( entitySetNamePresent ) {
                authorizedPropertyFqns = entityType.getProperties().stream()
                        .filter( propertyTypeFqn -> authzService.writePropertyTypeInEntitySet(
                                createEntityRequest.getEntitySetName().get(), propertyTypeFqn ) )
                        .collect( Collectors.toSet() );
            } else {
                authorizedPropertyFqns = entityType.getProperties().stream()
                        .filter( propertyTypeFqn -> authzService
                                .writePropertyTypeInEntityType( createEntityRequest.getEntityType(), propertyTypeFqn ) )
                        .collect( Collectors.toSet() );
            }
            
            dataService.createEntityData( createEntityRequest, authorizedPropertyFqns );
        } else {
            throw new ForbiddenException();
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
        //TODO skipping validation check for now - default @URL won't allow URLs without schema, may affect shuttle java.
        return dataService.getIntegrationScriptForUrl( urls );
    }

    @Override
    @RequestMapping(
        path = "/" + DataApi.INTEGRATION,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void createIntegrationScript( @RequestBody Map<String, String> integrationScripts ) {
        //TODO skipping validation check for now
        dataService.createIntegrationScript( integrationScripts );
        return null;
    }

}
