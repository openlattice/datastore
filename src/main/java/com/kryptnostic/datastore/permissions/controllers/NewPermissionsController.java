package com.kryptnostic.datastore.permissions.controllers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.spark_project.guava.collect.Iterables;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.dataloom.authorization.Ace;
import com.dataloom.authorization.AclData;
import com.dataloom.authorization.AclKey;
import com.dataloom.authorization.AclKeyInfo;
import com.dataloom.authorization.DetailedAclData;
import com.dataloom.authorization.HazelcastAuthorizationService;
import com.dataloom.authorization.NewPermissionsApi;
import com.dataloom.authorization.Principals;
import com.dataloom.authorization.SecurableObjectType;
import com.dataloom.authorization.requests.AclResponse;
import com.dataloom.authorization.requests.Action;
import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.authorization.requests.SecurableObject;
import com.dataloom.authorization.requests.SecurableObjectRequest;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.hazelcast.HazelcastMap;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.datastore.util.PermissionsResultsAdapter;

@RestController
public class NewPermissionsController implements NewPermissionsApi {

    @Inject
    private HazelcastAuthorizationService               authz;

    @Inject
    private HazelcastInstance                           hazelcastInstance;

    private final IMap<UUID, EntityType>          entityTypes = hazelcastInstance.getMap( HazelcastMap.ENTITY_TYPES.name() );
    private final IMap<String, EntitySet>         entitySets = hazelcastInstance.getMap( HazelcastMap.ENTITY_SETS.name() );
    private final IMap<FullQualifiedName, AclKey> fqns = hazelcastInstance.getMap( HazelcastMap.FQNS.name() );

    @Override
    @RequestMapping(
        path = "/" + PERMISSIONS,
        method = RequestMethod.GET,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Map<String, AclKeyInfo> getAdministerableEntitySets() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @RequestMapping(
        path = "/" + PERMISSIONS,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public AclResponse getAcl( SecurableObjectRequest req ) {
        List<AclKey> aclKeys = getKeyFromObject( req.getSecureObject() );

        Principal target;
        Set<Principal> principals;
        if ( req.getPrincipal().isPresent() ) {
            if ( !authz.checkIfUserIsOwner( aclKeys, Principals.getCurrentUser() ) ) {
                throw new UnauthorizedException(
                        "Only owner of a securable object can access other users' access rights." );
            }
            target = req.getPrincipal().get();
            principals = ImmutableSet.of( target );
        } else {
            target = Principals.getCurrentUser();
            principals = Principals.getCurrentPrincipals();
        }

        Set<Permission> result = authz.getSecurableObjectPermissions( aclKeys, principals );
        Map<List<SecurableObject>, Set<Permission>> aces = new HashMap<>();
        aces.put( req.getSecureObject(), result );

        return new AclResponse( target, aces );
    }

    @Override
    @RequestMapping(
        path = "/" + PERMISSIONS + "/" + DETAILS_BASE_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public AclResponse getDetailedAcl( SecurableObjectRequest req ) {
        if( req.getSecureObject().size() > 0 || req.getSecureObject().get( 0 ).getType() != SecurableObjectType.EntitySet ){
            throw new IllegalArgumentException( "Only entity set is supported for this endpoint.");
        }
        
        List<AclKey> aclKeys = getKeyFromObject( req.getSecureObject() );

        Principal target;
        Set<Principal> principals;
        if ( req.getPrincipal().isPresent() ) {
            if ( !authz.checkIfUserIsOwner( aclKeys, Principals.getCurrentUser() ) ) {
                throw new UnauthorizedException(
                        "Only owner of a securable object can access other users' access rights." );
            }
            target = req.getPrincipal().get();
            principals = ImmutableSet.of( target );
        } else {
            target = Principals.getCurrentUser();
            principals = Principals.getCurrentPrincipals();
        }

        FullQualifiedName etFqn = entitySets.get( aclKeys.get(0) ).getType();
        AclKey etKey = fqns.get( etFqn );
        EntityType et = entityTypes.get( etKey );

        Map<List<SecurableObject>, Set<Permission>> map = et.getProperties().stream()
                .collect( Collectors.toMap(
                        fqn -> getSecurableObjectWithPropertyType( req.getSecureObject(), fqn ),
                        fqn -> getPropertyTypeInEntitySetPermissions( aclKeys, fqn, principals ) ) );
        
        return new AclResponse( target, map );
    }

    @Override
    @RequestMapping(
        path = "/" + PERMISSIONS,
        method = RequestMethod.PATCH,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<AclResponse> updateAcl( SecurableObjectRequest req ) {
        Optional<Action> action = req.getAction();
        if ( !action.isPresent() ) {
            throw new IllegalArgumentException( "Action cannot be absent." );
        }
        Map<Principal, Set<Permission>> aces;
        if ( !req.getAces().isPresent() ) {
            throw new IllegalArgumentException( "Permissions to be updated cannot be absent." );
        } else {
            aces = req.getAces().get();
        }

        List<SecurableObject> secureObj = req.getSecureObject();
        List<AclKey> aclKeys = getKeyFromObject( secureObj );
        switch ( action.get() ) {
            case ADD:
                for ( Map.Entry<Principal, Set<Permission>> entry : aces.entrySet() ) {
                    authz.addPermission( aclKeys, entry.getKey(), entry.getValue() );
                }
                break;
            case SET:
                for ( Map.Entry<Principal, Set<Permission>> entry : aces.entrySet() ) {
                    authz.setPermission( aclKeys, entry.getKey(), entry.getValue() );
                }
                break;
            case REMOVE:
                for ( Map.Entry<Principal, Set<Permission>> entry : aces.entrySet() ) {
                    authz.removePermission( aclKeys, entry.getKey(), entry.getValue() );
                }
                break;
            default:
                break;
        }

        return Iterables.transform( authz.getAllSecurableObjectPermissions( aclKeys ),
                ace -> getResponseFromAce( secureObj, ace ) );
    }

    @Override
    @RequestMapping(
        path = "/" + PERMISSIONS + "/" + ALL_PATH,
        method = RequestMethod.POST,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    @ResponseStatus( HttpStatus.OK )
    public Iterable<AclResponse> getAllAcl( SecurableObjectRequest req ) {
        List<AclKey> aclKeys = getKeyFromObject( req.getSecureObject() );

        if ( !authz.checkIfUserIsOwner( aclKeys, Principals.getCurrentUser() ) ) {
            throw new UnauthorizedException(
                    "Only owner of a securable object can access other users' access rights." );
        }

        return Iterables.transform( authz.getAllSecurableObjectPermissions( aclKeys ),
                ace -> getResponseFromAce( req.getSecureObject(), ace ) );
    }

    /*******************
     * Helper methods
     ******************/

    private List<SecurableObject> getSecurableObjectWithPropertyType(
            List<SecurableObject> oldList,
            FullQualifiedName propertyTypeFqn ) {
        List<SecurableObject> newList = new ArrayList<>( oldList );
        newList.add( new SecurableObject( SecurableObjectType.PropertyTypeInEntitySet, propertyTypeFqn, null ) );
        return newList;
    }

    private Set<Permission> getPropertyTypeInEntitySetPermissions(
            List<AclKey> oldKeys,
            FullQualifiedName propertyTypeFqn,
            Set<Principal> principals ) {
        List<AclKey> newkey = new ArrayList<>( oldKeys );
        newkey.add( new AclKey(
                SecurableObjectType.PropertyTypeInEntitySet,
                fqns.get( propertyTypeFqn ).getId() ) );
        return authz.getSecurableObjectPermissions( newkey, principals );
    }

    private List<AclKey> getKeyFromObject( List<SecurableObject> secureObj ){
        return secureObj.stream().map( this::getKeyFromObject ).collect( Collectors.toList() );
    }
    
    private AclKey getKeyFromObject( SecurableObject secureObj ){
        if( secureObj.getFqn() != null ){
            return fqns.get( secureObj.getFqn() );
        } else if( secureObj.getName() != null ){
            EntitySet es = entitySets.get( secureObj.getName() );
            return es.getAclKey();
        }
        return null;
    }
    
    private AclResponse getResponseFromAce(
            List<SecurableObject> secureObj,
            Ace ace ) {
        Map<List<SecurableObject>, Set<Permission>> map = new HashMap<>();
        map.put( secureObj, ace.getPermissions() );
        return new AclResponse( ace.getPrincipal(), map );
    }
}
