package com.kryptnostic.datastore.data.controllers;

import com.kryptnostic.datastore.constants.DatastoreConstants;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.BadRequestException;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.Multimap;
import com.kryptnostic.conductor.rpc.LookupEntitiesRequest;
import com.kryptnostic.datastore.constants.CustomMediaType;
import com.kryptnostic.datastore.services.DataApi;
import com.kryptnostic.datastore.services.DataService;
import com.kryptnostic.datastore.services.requests.CreateEntityRequest;
import com.squareup.okhttp.Response;

@RestController
@RequestMapping( DataApi.CONTROLLER )
public class DataController implements DataApi {
    // TODO: Move to DataApi
    // TODO: Make spring handling case insensitive to follow correct Java style.
    public static enum FileType {
        json,
        csv;
    }

    @Inject
    private DataService dataService;

    @RequestMapping(
        path = { "/object/{id}" },
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, Object> getObject( @PathVariable( "id" ) UUID objectId ) {
        return dataService.getObject( objectId );
    }

    @RequestMapping(
        path = DataApi.ENTITY_DATA + DataApi.NAME_SPACE_PATH + DataApi.TYPE_NAME_PATH + DataApi.NAME_PATH,
        method = RequestMethod.GET,
        produces = { MediaType.APPLICATION_JSON_VALUE, CustomMediaType.TEXT_CSV_VALUE } )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfEntitySet(
            @PathVariable( NAME ) String entitySetName,
            @PathVariable( NAME_SPACE ) String entityTypeNamespace,
            @PathVariable( TYPE_NAME ) String entityTypeName,
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
        return dataService.getAllEntitiesOfEntitySet( entitySetName, entityTypeNamespace, entityTypeName );
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
        path = DataApi.ENTITY_DATA,
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
        return dataService.readAllEntitiesOfType( fqn );
    }

    @RequestMapping(
        path = DataApi.ENTITY_DATA + DataApi.FULLQUALIFIEDNAME_PATH_WITH_DOT,
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
        path = DataApi.ENTITY_DATA + DataApi.NAME_SPACE_PATH + DataApi.NAME_PATH,
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
        path = DataApi.ENTITY_DATA + DataApi.MULTIPLE,
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
        return dataService.readAllEntitiesOfSchema( fqns );
    }

    @Override
    @RequestMapping(
        path = DataApi.ENTITY_DATA + DataApi.FILTERED,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<UUID> getFilteredEntities( @RequestBody LookupEntitiesRequest lookupEntitiesRequest ) {
        return dataService.getFilteredEntities( lookupEntitiesRequest );
    }

    @Override
    @RequestMapping(
        path = DataApi.ENTITY_DATA,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response createEntityData( @RequestBody CreateEntityRequest createEntityRequest ) {
        dataService.createEntityData( createEntityRequest );
        return null;
    }

    @Override
    @RequestMapping(
        path = DataApi.INTEGRATION,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, String> getAllIntegrationScripts() {
        return dataService.getAllIntegrationScripts();
    }

    @Override
    @RequestMapping(
        path = DataApi.INTEGRATION,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, String> getIntegrationScript( @RequestBody Set<String> urls ) {
        return dataService.getIntegrationScriptForUrl( urls );
    }

    @Override
    @RequestMapping(
        path = DataApi.INTEGRATION,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response createIntegrationScript( @RequestBody Map<String, String> integrationScripts ) {
        dataService.createIntegrationScript( integrationScripts );
        return null;
    }

}
