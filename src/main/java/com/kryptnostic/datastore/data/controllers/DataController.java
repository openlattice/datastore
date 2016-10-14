package com.kryptnostic.datastore.data.controllers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

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
import com.kryptnostic.conductor.rpc.CreateEntityRequest;
import com.kryptnostic.conductor.rpc.LookupEntitiesRequest;
import com.kryptnostic.datastore.services.DataApi;
import com.kryptnostic.datastore.services.DataService;
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

    public static final String FILE_TYPE      = "fileType";

    @Inject
    private DataService        dataService;

    // TODO: Move this somewhere better
    public static final String MEDIA_TYPE_CSV = "text/csv";

    @RequestMapping(
        path = { "/object/{id}" },
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, Object> getObject( @PathVariable( "id" ) UUID objectId ) {
        return dataService.getObject( objectId );
    }

    @Override
    @RequestMapping(
        path = DataApi.ENTITYSET,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<UUID> getEntitySetOfType( @RequestBody FullQualifiedName fqn ) {
        return dataService.loadEntitySetOfType( fqn );
    }

    @Override
    @RequestMapping(
        path = DataApi.ENTITYSET + DataApi.FULLQUALIFIEDNAME_PATH_WITH_DOT,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<UUID> getEntitySetOfType( @PathVariable( FULLQUALIFIEDNAME ) String fqnString ) {
        return dataService.loadEntitySetOfType( new FullQualifiedName( fqnString ) );
    }

    @Override
    @RequestMapping(
        path = DataApi.ENTITYSET + DataApi.NAME_SPACE_PATH + DataApi.NAME_PATH,
        method = RequestMethod.GET,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<UUID> getEntitySetOfType(
            @PathVariable( NAME_SPACE ) String namespace,
            @PathVariable( NAME ) String name ) {
        return dataService.loadEntitySetOfType( new FullQualifiedName( namespace, name ) );
    }

    @Override
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfEntitySet(
            String entitySetName,
            String entityTypeName ) {
        return dataService.getAllEntitiesOfEntitySet( entitySetName, entityTypeName );
    }

    @RequestMapping(
        path = DataApi.ENTITYSET + DataApi.NAME_PATH + DataApi.TYPE_NAME_PATH + DataApi.ENTITY_DATA,
        method = RequestMethod.GET,
        produces = { MediaType.APPLICATION_JSON_VALUE, MEDIA_TYPE_CSV } )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfEntitySet(
            @PathVariable( NAME ) String entitySetName,
            @PathVariable( TYPE_NAME ) String entityTypeName,
            @RequestParam( FILE_TYPE ) FileType fileType,
            HttpServletResponse response ) {
        response.setHeader( "Content-Disposition",
                "attachment; filename=" + entitySetName + "." + fileType.toString() );
        setDownloadContentType( response, fileType );
        return getAllEntitiesOfEntitySet( entitySetName, entityTypeName );
    }

    private static void setDownloadContentType( HttpServletResponse response, FileType fileType ) {
        switch ( fileType ) {
            case json:
                response.setContentType( MediaType.APPLICATION_JSON_VALUE );
            case csv:
                response.setContentType( MEDIA_TYPE_CSV );
            default:
                response.setContentType( MediaType.APPLICATION_JSON_VALUE );

        }
    }

    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType(
            FullQualifiedName fqn ) {
        return dataService.readAllEntitiesOfType( fqn );
    }

    @RequestMapping(
        path = DataApi.ENTITY_DATA,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = { MediaType.APPLICATION_JSON_VALUE, MEDIA_TYPE_CSV } )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType(
            @RequestBody FullQualifiedName fqn,
            @RequestParam( FILE_TYPE ) FileType fileType,
            HttpServletResponse response ) {
        response.setHeader( "Content-Disposition",
                "attachment; filename=" + fqn.getNamespace() + "_" + fqn.getName() + "." + fileType.toString() );
        setDownloadContentType( response, fileType );
        return getAllEntitiesOfType( fqn );
    }

    @Override
    public Iterable<Iterable<Multimap<FullQualifiedName, Object>>> getAllEntitiesOfTypes(
            List<FullQualifiedName> fqns ) {
        return dataService.readAllEntitiesOfSchema( fqns );
    }

    @RequestMapping(
        path = DataApi.ENTITY_DATA + DataApi.MULTIPLE,
        method = RequestMethod.PUT,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Iterable<Multimap<FullQualifiedName, Object>>> getAllEntitiesOfTypes(
            @RequestBody List<FullQualifiedName> fqns,
            HttpServletResponse response ) {
        response.setHeader( "Content-Disposition", "attachment; filename=entities_data" + ".json" );
        return getAllEntitiesOfTypes( fqns );
    }

    @RequestMapping(
        path = DataApi.ENTITY_DATA + DataApi.FULLQUALIFIEDNAME_PATH_WITH_DOT,
        method = RequestMethod.GET,
        produces = { MediaType.APPLICATION_JSON_VALUE, MEDIA_TYPE_CSV } )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType(
            @PathVariable( FULLQUALIFIEDNAME ) String fqnAsString,
            @RequestParam( FILE_TYPE ) FileType fileType,
            HttpServletResponse response ) {

        FullQualifiedName fqn = new FullQualifiedName( fqnAsString );
        response.setHeader( "Content-Disposition",
                "attachment; filename=" + fqn.getNamespace() + "_" + fqn.getName() + "." + fileType.toString() );
        setDownloadContentType( response, fileType );
        return getAllEntitiesOfType( fqn );
    }

    @RequestMapping(
        path = DataApi.ENTITY_DATA + DataApi.NAME_SPACE_PATH + DataApi.NAME_PATH,
        method = RequestMethod.GET,
        produces = { MediaType.APPLICATION_JSON_VALUE, MEDIA_TYPE_CSV } )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType(
            @PathVariable( NAME_SPACE ) String namespace,
            @PathVariable( NAME ) String name,
            @RequestParam( FILE_TYPE ) FileType fileType,
            HttpServletResponse response ) {
        response.setHeader( "Content-Disposition",
                "attachment; filename=" + namespace + "_" + name + "." + fileType.toString() );
        setDownloadContentType( response, fileType );
        return getAllEntitiesOfType( new FullQualifiedName( namespace, name ) );
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

    @Override
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType( String fqnAsString ) {
        return getAllEntitiesOfType( new FullQualifiedName( fqnAsString ) );
    }

    @Override
    public Iterable<Multimap<FullQualifiedName, Object>> getAllEntitiesOfType( String namespace, String name ) {
        return getAllEntitiesOfType( new FullQualifiedName( namespace, name ) );
    }

}
