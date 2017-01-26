package com.dataloom.datastore.requests.controllers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.ws.rs.ForbiddenException;

import com.dataloom.authorization.*;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dataloom.requests.HazelcastRequestsManager;
import com.dataloom.requests.Request;
import com.dataloom.requests.RequestStatus;
import com.dataloom.requests.RequestsApi;
import com.dataloom.requests.Status;
import com.dataloom.requests.util.RequestUtil;

import retrofit2.http.Body;
import retrofit2.http.Path;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
@RestController
@RequestMapping( RequestsApi.CONTROLLER )
public class RequestsController implements RequestsApi, AuthorizingComponent {

    @Inject
    private AuthorizationManager     authorizations;

    @Inject
    private HazelcastRequestsManager hrm;

    @Override
    public AuthorizationManager getAuthorizationManager() {
        return authorizations;
    }

    @Override
    @GetMapping(
        path = { "", "/" } )
    public Iterable<Status> getMyRequests() {
        return hrm.getStatuses( Principals.getCurrentUser() )::iterator;
    }

    @Override
    @GetMapping(
        path = REQUEST_STATUS_PATH )
    public Iterable<Status> getMyRequests( @PathVariable( REQUEST_STATUS ) RequestStatus requestStatus ) {
        return hrm.getStatuses( Principals.getCurrentUser(), requestStatus )::iterator;
    }

    @Override
    @PutMapping(
        path = { "", "/" } )
    public Void submit( @RequestBody Set<Request> requests ) {
        Map<AceKey, Status> statusMap = RequestUtil.reqsAsStatusMap( requests );
        hrm.submitAll( statusMap );
        return null;
    }

    @Override
    @PostMapping(
        path = { "", "/" },
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Status> getStatuses( @RequestBody Set<List<UUID>> aclKeys ) {
        return aclKeys.stream().flatMap( this::getStatuses )::iterator;
    }

    @Override
    @PostMapping(
        path = REQUEST_STATUS_PATH,
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Iterable<Status> getStatuses(
            @PathVariable( REQUEST_STATUS ) RequestStatus requestStatus,
            @RequestBody Set<List<UUID>> aclKeys ) {
        return aclKeys.stream().flatMap( getStatusesInStatus( requestStatus ) )::iterator;
    }

    @Override
    @PatchMapping(
        path = { "", "/" },
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE )
    public Void updateStatuses( @RequestBody Set<Status> statuses ) {
        if ( statuses.stream().map( Status::getAclKey ).allMatch( this::owns ) ) {
            Map<AceKey, Status> statusMap = RequestUtil.statusMap( statuses );
            for( Status status : statuses ) {
                Map<AceKey,Status> s = ImmutableMap.of( RequestUtil.aceKey( status ), status);
                hrm.submitAll( s );
            }
            return null;
        }
        throw new ForbiddenException();
    }

    private Function<List<UUID>, Stream<Status>> getStatusesInStatus( RequestStatus requestStatus ) {
        return aclKey -> owns( aclKey ) ? hrm.getStatusesForAllUser( aclKey, requestStatus )
                : hrm.getStatuses( Stream.of( new AceKey( aclKey, Principals.getCurrentUser() ) ) )
                        .filter( Predicates.notNull()::apply)
                        .filter( status -> status.getStatus().equals( requestStatus ) );
    }

    private Stream<Status> getStatuses( List<UUID> aclKey ) {
        return owns( aclKey ) ? hrm.getStatusesForAllUser( aclKey )
                : hrm.getStatuses( Stream.of( new AceKey( aclKey, Principals.getCurrentUser() ) ) );
    }
}
