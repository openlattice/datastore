package com.kryptnostic.datastore.permissions.controllers;

import java.util.Set;

import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.kryptnostic.datastore.services.ActionAuthorizationService;
import com.kryptnostic.datastore.services.PermissionsApi;
import com.kryptnostic.datastore.services.PermissionsService;
import com.kryptnostic.datastore.services.requests.EntityTypeAclRequest;
import com.kryptnostic.datastore.services.requests.EntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntitySetAclRemovalRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntityTypeAclRemovalRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntityTypeAclRequest;

import retrofit.client.Response;

@RestController
public class PermissionsController implements PermissionsApi {

    @Inject
    private PermissionsService         ps;

    @Inject
    private ActionAuthorizationService authzService;

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response updateEntityTypesAcls( @RequestBody Set<EntityTypeAclRequest> requests ) {
        if ( authzService.updateEntityTypesAcls() ) {
            for ( EntityTypeAclRequest request : requests ) {
                switch ( request.getAction() ) {
                    case ADD:
                        ps.addPermissionsForEntityType( request.getRole(), request.getType(), request.getPermissions() );
                        break;
                    case SET:
                        ps.setPermissionsForEntityType( request.getRole(), request.getType(), request.getPermissions() );
                        break;
                    case REMOVE:
                        ps.removePermissionsForEntityType( request.getRole(),
                                request.getType(),
                                request.getPermissions() );
                        break;
                }
            }
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_SETS_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response updateEntitySetsAcls( @RequestBody Set<EntitySetAclRequest> requests ) {
        if ( authzService.updateEntitySetsAcls() ) {
            for ( EntitySetAclRequest request : requests ) {
                switch ( request.getAction() ) {
                    case ADD:
                        ps.addPermissionsForEntitySet( request.getRole(),
                                request.getName(),
                                request.getPermissions() );
                        break;
                    case SET:
                        ps.setPermissionsForEntitySet( request.getRole(),
                                request.getName(),
                                request.getPermissions() );
                        break;
                    case REMOVE:
                        ps.removePermissionsForEntitySet( request.getRole(),
                                request.getName(),
                                request.getPermissions() );
                        break;
                }
            }
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_TYPE_BASE_PATH + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response updatePropertyTypeInEntityTypeAcls(
            @RequestBody Set<PropertyTypeInEntityTypeAclRequest> requests ) {
        if ( authzService.updatePropertyTypeInEntityTypeAcls() ) {
            for ( PropertyTypeInEntityTypeAclRequest request : requests ) {
                switch ( request.getAction() ) {
                    case ADD:
                        ps.addPermissionsForPropertyTypeInEntityType( request.getRole(),
                                request.getType(),
                                request.getPropertyType(),
                                request.getPermissions() );
                        break;
                    case SET:
                        ps.setPermissionsForPropertyTypeInEntityType( request.getRole(),
                                request.getType(),
                                request.getPropertyType(),
                                request.getPermissions() );
                        break;
                    case REMOVE:
                        ps.removePermissionsForPropertyTypeInEntityType( request.getRole(),
                                request.getType(),
                                request.getPropertyType(),
                                request.getPermissions() );
                        break;
                }
            }
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_SETS_BASE_PATH + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response updatePropertyTypeInEntitySetAcls( @RequestBody Set<PropertyTypeInEntitySetAclRequest> requests ) {
        if ( authzService.updatePropertyTypeInEntitySetAcls() ) {
            for ( PropertyTypeInEntitySetAclRequest request : requests ) {
                switch ( request.getAction() ) {
                    case ADD:
                        ps.addPermissionsForPropertyTypeInEntitySet( request.getRole(),
                                request.getName(),
                                request.getPropertyType(),
                                request.getPermissions() );
                        break;
                    case SET:
                        ps.setPermissionsForPropertyTypeInEntitySet( request.getRole(),
                                request.getName(),
                                request.getPropertyType(),
                                request.getPermissions() );
                        break;
                    case REMOVE:
                        ps.removePermissionsForPropertyTypeInEntitySet( request.getRole(),
                                request.getName(),
                                request.getPropertyType(),
                                request.getPermissions() );
                        break;
                }
            }
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_TYPE_BASE_PATH,
        method = RequestMethod.DELETE,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response removeEntityTypeAcls( @RequestBody Set<FullQualifiedName> entityTypeFqns ) {
        if ( authzService.removeEntityTypeAcls() ) {
            for ( FullQualifiedName entityTypeFqn : entityTypeFqns ) {
                ps.removePermissionsForEntityType( entityTypeFqn );
            }
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_SETS_BASE_PATH,
        method = RequestMethod.DELETE,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response removeEntitySetAcls( @RequestBody Set<String> entitySetNames ) {
        if ( authzService.removeEntitySetAcls() ) {
            for ( String entitySetName : entitySetNames ) {
                ps.removePermissionsForEntitySet( entitySetName );
            }
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_TYPE_BASE_PATH + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.DELETE,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response removePropertyTypeInEntityTypeAcls(
            @RequestBody Set<PropertyTypeInEntityTypeAclRemovalRequest> requests ) {
        if ( authzService.removePropertyTypeInEntityTypeAcls() ) {
            for ( PropertyTypeInEntityTypeAclRemovalRequest request : requests ) {
                for ( FullQualifiedName propertyTypeFqn : request.getProperties() ) {
                    ps.removePermissionsForPropertyTypeInEntityType( request.getType(), propertyTypeFqn );
                }
            }
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_TYPE_BASE_PATH + PROPERTY_TYPE_BASE_PATH + ALL_PATH,
        method = RequestMethod.DELETE,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response removeAllPropertyTypesInEntityTypeAcls( @RequestBody Set<FullQualifiedName> entityTypeFqns ) {
        if ( authzService.removeAllPropertyTypesInEntityTypeAcls() ) {
            for ( FullQualifiedName entityTypeFqn : entityTypeFqns ) {
                ps.removePermissionsForPropertyTypeInEntityType( entityTypeFqn );
            }
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_SETS_BASE_PATH + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.DELETE,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response removePropertyTypeInEntitySetAcls(
            @RequestBody Set<PropertyTypeInEntitySetAclRemovalRequest> requests ) {
        if ( authzService.removePropertyTypeInEntitySetAcls() ) {
            for ( PropertyTypeInEntitySetAclRemovalRequest request : requests ) {
                for ( FullQualifiedName propertyTypeFqn : request.getProperties() ) {
                    ps.removePermissionsForPropertyTypeInEntitySet(
                            request.getName(),
                            propertyTypeFqn );
                }
            }
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_SETS_BASE_PATH + PROPERTY_TYPE_BASE_PATH + ALL_PATH,
        method = RequestMethod.DELETE,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response removeAllPropertyTypesInEntitySetAcls( @RequestBody Set<String> entitySetNames ) {
        if ( authzService.removeAllPropertyTypesInEntitySetAcls() ) {
            for ( String entitySetName : entitySetNames ) {
                ps.removePermissionsForPropertyTypeInEntitySet( entitySetName );
            }
        }
        return null;
    }
}
