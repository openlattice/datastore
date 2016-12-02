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

import com.dataloom.authorization.requests.EntitySetAclRequest;
import com.dataloom.authorization.requests.EntityTypeAclRequest;
import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.PermissionsInfo;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.authorization.requests.PrincipalType;
import com.dataloom.authorization.requests.PropertyTypeInEntitySetAclRemovalRequest;
import com.dataloom.authorization.requests.PropertyTypeInEntitySetAclRequestWithRequestingUser;
import com.dataloom.authorization.requests.PropertyTypeInEntityTypeAclRemovalRequest;
import com.dataloom.authorization.requests.PropertyTypeInEntityTypeAclRequest;
import com.dataloom.edm.requests.PropertyTypeInEntitySetAclRequest;
import com.dataloom.permissions.PermissionsApi;
import com.google.common.collect.Iterables;
import com.kryptnostic.datastore.exceptions.ResourceNotFoundException;
import com.kryptnostic.datastore.services.ActionAuthorizationService;
import com.kryptnostic.datastore.services.PermissionsService;
import com.kryptnostic.datastore.util.PermissionsResultsAdapter;
import com.kryptnostic.instrumentation.v1.exceptions.types.UnauthorizedException;

@RestController
public class PermissionsController implements PermissionsApi {

    @Inject
    private PermissionsService         ps;

    @Inject
    private ActionAuthorizationService authzService;

    @Inject
    private PermissionsResultsAdapter adapter;

    @Override
    @RequestMapping(
        path = "/" + CONTROLLER + "/" + ENTITY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void updateEntityTypesAcls( @RequestBody Set<EntityTypeAclRequest> requests ) {
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
        path = "/" + CONTROLLER + "/" + ENTITY_SETS_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void updateEntitySetsAcls( @RequestBody Set<EntitySetAclRequest> requests ) {
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
        path = "/" + CONTROLLER + "/" + ENTITY_TYPE_BASE_PATH + "/" + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void updatePropertyTypeInEntityTypeAcls(
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
        path = "/" + CONTROLLER + "/" + ENTITY_SETS_BASE_PATH + "/" + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void updatePropertyTypeInEntitySetAcls( @RequestBody Set<PropertyTypeInEntitySetAclRequest> requests ) {
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
        path = "/" + CONTROLLER + "/" + ENTITY_TYPE_BASE_PATH,
        method = RequestMethod.DELETE,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void removeEntityTypeAcls( @RequestBody Set<FullQualifiedName> entityTypeFqns ) {
        if ( authzService.removeEntityTypeAcls() ) {
            for ( FullQualifiedName entityTypeFqn : entityTypeFqns ) {
                ps.removePermissionsForEntityType( entityTypeFqn );
            }
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + CONTROLLER + "/" + ENTITY_SETS_BASE_PATH,
        method = RequestMethod.DELETE,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void removeEntitySetAcls( @RequestBody Set<String> entitySetNames ) {
        if ( authzService.removeEntitySetAcls() ) {
            for ( String entitySetName : entitySetNames ) {
                ps.removePermissionsForEntitySet( entitySetName );
            }
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + CONTROLLER + "/" + ENTITY_TYPE_BASE_PATH + "/" + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.DELETE,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void removePropertyTypeInEntityTypeAcls(
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
        path = "/" + CONTROLLER + "/" + ENTITY_TYPE_BASE_PATH + "/" + PROPERTY_TYPE_BASE_PATH + "/" + ALL_PATH,
        method = RequestMethod.DELETE,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void removeAllPropertyTypesInEntityTypeAcls( @RequestBody Set<FullQualifiedName> entityTypeFqns ) {
        if ( authzService.removeAllPropertyTypesInEntityTypeAcls() ) {
            for ( FullQualifiedName entityTypeFqn : entityTypeFqns ) {
                ps.removePermissionsForPropertyTypeInEntityType( entityTypeFqn );
            }
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + CONTROLLER + "/" + ENTITY_SETS_BASE_PATH + "/" + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.DELETE,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void removePropertyTypeInEntitySetAcls(
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
        path = "/" + CONTROLLER + "/" + ENTITY_SETS_BASE_PATH + "/" + PROPERTY_TYPE_BASE_PATH + "/" + ALL_PATH,
        method = RequestMethod.DELETE,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void removeAllPropertyTypesInEntitySetAcls( @RequestBody Set<String> entitySetNames ) {
        if ( authzService.removeAllPropertyTypesInEntitySetAcls() ) {
            for ( String entitySetName : entitySetNames ) {
                ps.removePermissionsForPropertyTypeInEntitySet( entitySetName );
            }
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + CONTROLLER + "/" + ENTITY_SETS_BASE_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public EnumSet<Permission> getEntitySetAclsForUser( @RequestParam( NAME ) String entitySetName ) {
        if ( authzService.getEntitySet( entitySetName ) ) {
            return ps.getEntitySetAclsForUser( authzService.getUserId(), authzService.getRoles(), entitySetName );
        } else {
            throw new ResourceNotFoundException( "Entity Set not found." );
        }
    }

    @Override
    @RequestMapping(
        path = "/" + CONTROLLER + "/" + ENTITY_SETS_BASE_PATH + "/" + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntitySetAclsForUser(
            @RequestParam( NAME ) String entitySetName ) {
        if ( authzService.getEntitySet( entitySetName ) ) {
            return ps.getPropertyTypesInEntitySetAclsForUser( authzService.getUserId(),
                    authzService.getRoles(),
                    entitySetName );
        } else {
            throw new ResourceNotFoundException( "Entity Set not found." );
        }
    }

    @Override
    @RequestMapping(
        path = "/" + CONTROLLER + "/" + ENTITY_TYPE_BASE_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public EnumSet<Permission> getEntityTypeAclsForUser(
            @RequestParam( NAMESPACE ) String entityTypeNamespace,
            @RequestParam( NAME ) String entityTypeName ) {
        return ps.getEntityTypeAclsForUser( authzService.getUserId(),
                authzService.getRoles(),
                new FullQualifiedName( entityTypeNamespace, entityTypeName ) );
    }

    @Override
    @RequestMapping(
        path = "/" + CONTROLLER + "/" + ENTITY_TYPE_BASE_PATH + "/" + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Map<FullQualifiedName, EnumSet<Permission>> getPropertyTypesInEntityTypeAclsForUser(
            @RequestParam( NAMESPACE ) String entityTypeNamespace,
            @RequestParam( NAME ) String entityTypeName ) {
        return ps.getPropertyTypesInEntityTypeAclsForUser( authzService.getUserId(),
                authzService.getRoles(),
                new FullQualifiedName( entityTypeNamespace, entityTypeName ) );
    }

    @Override
    @RequestMapping(
        path = "/" + CONTROLLER + "/" + ENTITY_SETS_BASE_PATH + "/" + OWNER_PATH,
        method = RequestMethod.GET )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PermissionsInfo> getEntitySetAclsForOwner( @RequestParam( NAME ) String entitySetName ) {
        if ( authzService.getEntitySetAclsForOwner( entitySetName ) ) {
            return Iterables.transform( ps.getEntitySetAclsForOwner( entitySetName ), adapter::mapUserIdToName );
        } else {
            // TODO to write a new handler
            throw new UnauthorizedException();
        }
    }

    @Override
    @RequestMapping(
        path = "/" + CONTROLLER + "/" + ENTITY_SETS_BASE_PATH + "/" + OWNER_PATH,
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
        path = "/" + CONTROLLER + "/" + ENTITY_SETS_BASE_PATH + "/" + OWNER_PATH + "/" + PROPERTY_TYPE_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PermissionsInfo> getPropertyTypesInEntitySetAclsForOwner(
            @RequestParam( NAME ) String entitySetName,
            @RequestBody FullQualifiedName propertyTypeFqn ) {
        if ( authzService.getEntitySetAclsForOwner( entitySetName ) ) {
            return Iterables.transform(  ps.getPropertyTypesInEntitySetAclsForOwner( entitySetName, propertyTypeFqn ), adapter::mapUserIdToName );
        } else {
            // TODO to write a new handler
            throw new UnauthorizedException();
        }
    }

    @Override
    @RequestMapping(
        path = "/" + CONTROLLER + "/" + ENTITY_SETS_BASE_PATH + "/" + REQUEST_PERMISSIONS_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Void addPermissionsRequestForPropertyTypesInEntitySet(
            @RequestBody Set<PropertyTypeInEntitySetAclRequest> requests ) {

        String userId = authzService.getUserId();
        Principal userAsPrincipal = new Principal( PrincipalType.USER ).setId( userId );

        for ( PropertyTypeInEntitySetAclRequest request : requests ) {
            switch ( request.getAction() ) {
                case REQUEST:
                    // if principal is missing, would assume that user is requesting permissions for himself
                    ps.addPermissionsRequestForPropertyTypeInEntitySet( userId,
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
        path = "/" + CONTROLLER + "/" + ENTITY_SETS_BASE_PATH + "/" + REQUEST_PERMISSIONS_PATH,
        method = RequestMethod.DELETE )
    @ResponseStatus( HttpStatus.OK )
    public Void removePermissionsRequestForEntitySet(
            @RequestParam( REQUEST_ID ) UUID id ) {
        if ( authzService.removePermissionsRequestForEntitySet( id ) ) {
            ps.removePermissionsRequestForEntitySet( id );
        } else {
            // TODO write an error handler
            throw new UnauthorizedException();
        }
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + CONTROLLER + "/" + ENTITY_SETS_BASE_PATH + "/" + OWNER_PATH + "/" + REQUEST_PERMISSIONS_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PropertyTypeInEntitySetAclRequestWithRequestingUser> getAllReceivedRequestsForPermissions(
            @RequestParam(
                value = NAME,
                required = false ) String entitySetName ) {
        String userId = authzService.getUserId();

        if ( entitySetName != null && !entitySetName.isEmpty() ) {
            if ( authzService.getAllReceivedRequestsForPermissions( entitySetName ) ) {
                return Iterables.transform( ps.getAllReceivedRequestsForPermissionsOfEntitySet( entitySetName ), adapter::mapUserIdToName );
            } else {
                throw new ResourceNotFoundException( "Entity Set Not Found." );
            }
        } else {
            return Iterables.transform( ps.getAllReceivedRequestsForPermissionsOfUserId( userId ), adapter::mapUserIdToName );
        }
    }

    @Override
    @RequestMapping(
        path = "/" + CONTROLLER + "/" + ENTITY_SETS_BASE_PATH + "/" + REQUEST_PERMISSIONS_PATH,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<PropertyTypeInEntitySetAclRequestWithRequestingUser> getAllSentRequestsForPermissions(
            @RequestParam(
                value = NAME,
                required = false ) String entitySetName ) {
        String userId = authzService.getUserId();

        if ( entitySetName != null && !entitySetName.isEmpty() ) {
            return Iterables.transform( ps.getAllSentRequestsForPermissions( userId, entitySetName ), adapter::mapUserIdToName );
        } else {
            return Iterables.transform( ps.getAllSentRequestsForPermissions( userId ), adapter::mapUserIdToName );
        }
    }

}
