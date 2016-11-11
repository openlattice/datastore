package com.kryptnostic.datastore.permissions.controllers;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.kryptnostic.datastore.Permission;
import com.kryptnostic.datastore.PermissionsInfo;
import com.kryptnostic.datastore.Principal;
import com.kryptnostic.datastore.PrincipalType;
import com.kryptnostic.datastore.exceptions.ResourceNotFoundException;
import com.kryptnostic.datastore.services.ActionAuthorizationService;
import com.kryptnostic.datastore.services.PermissionsApi;
import com.kryptnostic.datastore.services.PermissionsService;
import com.kryptnostic.datastore.services.requests.EntityTypeAclRequest;
import com.kryptnostic.datastore.services.requests.EntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntitySetAclRemovalRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntitySetAclRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntityTypeAclRemovalRequest;
import com.kryptnostic.datastore.services.requests.PropertyTypeInEntityTypeAclRequest;
import com.kryptnostic.instrumentation.v1.exceptions.types.UnauthorizedException;

import retrofit.client.Response;
import retrofit.http.DELETE;
import retrofit.http.GET;
import retrofit.http.POST;
import retrofit.http.Query;

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
                        ps.addPermissionsForEntityType( request.getPrincipal(),
                                request.getType(),
                                request.getPermissions() );
                        break;
                    case SET:
                        ps.setPermissionsForEntityType( request.getPrincipal(),
                                request.getType(),
                                request.getPermissions() );
                        break;
                    case REMOVE:
                        ps.removePermissionsForEntityType( request.getPrincipal(),
                                request.getType(),
                                request.getPermissions() );
                        break;
                    default:
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
                        ps.addPermissionsForEntitySet( request.getPrincipal(),
                                request.getName(),
                                request.getPermissions() );
                        break;
                    case SET:
                        ps.setPermissionsForEntitySet( request.getPrincipal(),
                                request.getName(),
                                request.getPermissions() );
                        break;
                    case REMOVE:
                        ps.removePermissionsForEntitySet( request.getPrincipal(),
                                request.getName(),
                                request.getPermissions() );
                        break;
                    default:
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
                        ps.addPermissionsForPropertyTypeInEntityType( request.getPrincipal(),
                                request.getType(),
                                request.getPropertyType(),
                                request.getPermissions() );
                        break;
                    case SET:
                        ps.setPermissionsForPropertyTypeInEntityType( request.getPrincipal(),
                                request.getType(),
                                request.getPropertyType(),
                                request.getPermissions() );
                        break;
                    case REMOVE:
                        ps.removePermissionsForPropertyTypeInEntityType( request.getPrincipal(),
                                request.getType(),
                                request.getPropertyType(),
                                request.getPermissions() );
                        break;
                    default:
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
                        ps.addPermissionsForPropertyTypeInEntitySet( request.getPrincipal(),
                                request.getName(),
                                request.getPropertyType(),
                                request.getPermissions() );
                        break;
                    case SET:
                        ps.setPermissionsForPropertyTypeInEntitySet( request.getPrincipal(),
                                request.getName(),
                                request.getPropertyType(),
                                request.getPermissions() );
                        break;
                    case REMOVE:
                        ps.removePermissionsForPropertyTypeInEntitySet( request.getPrincipal(),
                                request.getName(),
                                request.getPropertyType(),
                                request.getPermissions() );
                        break;
                    default:
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
        //Debug by Ho Chung
        method = RequestMethod.POST,
//        method = RequestMethod.DELETE,
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
        //Debug by Ho Chung
        method = RequestMethod.POST,    
//        method = RequestMethod.DELETE,
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

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_SETS_BASE_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public EnumSet<Permission> getEntitySetAclsForUser( @RequestParam( NAME ) String entitySetName ) {
        if ( authzService.getEntitySet( entitySetName ) ) {
            return ps.getEntitySetAclsForUser( authzService.getUsername(), authzService.getRoles(), entitySetName );
        } else {
            throw new ResourceNotFoundException( "Entity Set not found." );
        }
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_SETS_BASE_PATH + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntitySetAclsForUser(
            @RequestParam( NAME ) String entitySetName ) {
        if ( authzService.getEntitySet( entitySetName ) ) {
            return ps.getPropertyTypesInEntitySetAclsForUser( authzService.getUsername(),
                    authzService.getRoles(),
                    entitySetName );
        } else {
            throw new ResourceNotFoundException( "Entity Set not found." );
        }
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_TYPE_BASE_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public EnumSet<Permission> getEntityTypeAclsForUser(
            @RequestParam( NAMESPACE ) String entityTypeNamespace,
            @RequestParam( NAME ) String entityTypeName ) {
        return ps.getEntityTypeAclsForUser( authzService.getUsername(),
                authzService.getRoles(),
                new FullQualifiedName( entityTypeNamespace, entityTypeName ) );
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_TYPE_BASE_PATH + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntityTypeAclsForUser(
            @RequestParam( NAMESPACE ) String entityTypeNamespace,
            @RequestParam( NAME ) String entityTypeName ) {
        return ps.getPropertyTypesInEntityTypeAclsForUser( authzService.getUsername(),
                authzService.getRoles(),
                new FullQualifiedName( entityTypeNamespace, entityTypeName ) );
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_SETS_BASE_PATH + OWNER_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PermissionsInfo> getEntitySetAclsForOwner( @RequestParam( NAME ) String entitySetName ) {
        if ( authzService.getEntitySetAclsForOwner( entitySetName ) ) {
            return ps.getEntitySetAclsForOwner( entitySetName );
        } else {
            // TODO to write a new handler
            throw new UnauthorizedException();
        }
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_SETS_BASE_PATH + OWNER_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntitySetAclsForOwner(
            @RequestParam( NAME ) String entitySetName,
            @RequestBody Principal principal ) {
        if ( authzService.getEntitySetAclsForOwner( entitySetName ) ) {
            return ps.getPropertyTypesInEntitySetAclsOfPrincipalForOwner( entitySetName, principal );
        } else {
            // TODO to write a new handler
            throw new UnauthorizedException();
        }
    }
    
    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_SETS_BASE_PATH + OWNER_PATH + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PermissionsInfo> getPropertyTypesInEntitySetAclsForOwner(
            @RequestParam( NAME ) String entitySetName,
            @RequestBody FullQualifiedName propertyTypeFqn ) {
        if ( authzService.getEntitySetAclsForOwner( entitySetName ) ) {
            return ps.getPropertyTypesInEntitySetAclsForOwner( entitySetName, propertyTypeFqn );
        } else {
            // TODO to write a new handler
            throw new UnauthorizedException();
        }
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_SETS_BASE_PATH + REQUEST_PERMISSIONS_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Response addPermissionsRequestForPropertyTypesInEntitySet(
           @RequestBody Set<PropertyTypeInEntitySetAclRequest> requests ) {
        
        String username = authzService.getUsername();
        Principal userAsPrincipal = new Principal(PrincipalType.USER, username);
        
        for ( PropertyTypeInEntitySetAclRequest request : requests ) {
            switch ( request.getAction() ) {
                case REQUEST:
                    // if principal is missing, would assume that user is requesting permissions for himself
                    ps.addPermissionsRequestForPropertyTypeInEntitySet( username,
                            ( request.getPrincipal() != null ) ? request.getPrincipal() : userAsPrincipal,
                            request.getName(),
                            request.getPropertyType(),
                            request.getPermissions() );
                    break;
                default:
                    break;
            }
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_SETS_BASE_PATH + REQUEST_PERMISSIONS_PATH,
        method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Response removePermissionsRequestForEntitySet(
            @RequestParam( REQUEST_ID ) UUID id ) {
        if( authzService.removePermissionsRequestForEntitySet( id ) ){
            ps.removePermissionsRequestForEntitySet( id );
        } else {
            //TODO write an error handler
            throw new UnauthorizedException();
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_SETS_BASE_PATH + OWNER_PATH + REQUEST_PERMISSIONS_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PropertyTypeInEntitySetAclRequest> getAllReceivedRequestsForPermissions(
            @RequestParam( value = NAME, required = false ) String entitySetName ) {
        String username = authzService.getUsername();
        
        if( entitySetName != null && !entitySetName.isEmpty() ){
            if( authzService.getAllReceivedRequestsForPermissions( entitySetName ) ){
                return ps.getAllReceivedRequestsForPermissionsOfEntitySet( entitySetName );
            } else {
                throw new ResourceNotFoundException("Entity Set Not Found.");
            }
        } else {
            return ps.getAllReceivedRequestsForPermissionsOfUsername( username );
        }
    }
    
    @Override
    @RequestMapping(
        path = CONTROLLER + ENTITY_SETS_BASE_PATH + REQUEST_PERMISSIONS_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PropertyTypeInEntitySetAclRequest> getAllSentRequestsForPermissions(
            @RequestParam( value = NAME, required = false ) String entitySetName ) {
        String username = authzService.getUsername();
        
        if( entitySetName != null && !entitySetName.isEmpty() ){
            return ps.getAllSentRequestsForPermissions( username, entitySetName );
        } else {
            return ps.getAllSentRequestsForPermissions( username );
        }
    }

}
