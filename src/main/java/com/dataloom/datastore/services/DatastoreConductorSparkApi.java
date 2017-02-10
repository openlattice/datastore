package com.dataloom.datastore.services;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.auth0.jwt.internal.org.apache.commons.lang3.NotImplementedException;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.data.requests.LookupEntitiesRequest;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.organization.Organization;
import com.google.common.base.Optional;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;
import com.kryptnostic.conductor.rpc.QueryResult;

public class DatastoreConductorSparkApi implements ConductorSparkApi {

    @Override
    public QueryResult getAllEntitiesOfType( FullQualifiedName entityTypeFqn ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public QueryResult getAllEntitiesOfType(
            FullQualifiedName entityTypeFqn,
            List<PropertyType> authorizedProperties ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public QueryResult getAllEntitiesOfEntitySet( FullQualifiedName entityFqn, String entitySetName ) {

        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public QueryResult getAllEntitiesOfEntitySet(
            FullQualifiedName entityFqn,
            String entitySetName,
            List<PropertyType> authorizedProperties ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public QueryResult getFilterEntities( LookupEntitiesRequest request ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public Boolean submitEntitySetToElasticsearch(
            EntitySet entitySet,
            List<PropertyType> propertyTypes,
            Principal principal ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public Boolean deleteEntitySet( UUID entitySetId ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public List<Map<String, Object>> executeElasticsearchMetadataQuery(
            Optional<String> query,
            Optional<UUID> optionalEntityType,
            Optional<Set<UUID>> optionalPropertyTypes,
            Set<Principal> principals ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override public Boolean updateEntitySetMetadata( EntitySet entitySet ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override public Boolean updatePropertyTypesInEntitySet(
            UUID entitySetId, Set<PropertyType> newPropertyTypes ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public Boolean updateEntitySetPermissions(
            UUID entitySetId,
            Principal principal,
            Set<Permission> permissions ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public Boolean updateEntitySetMetadata( EntitySet entitySet ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public Boolean updatePropertyTypesInEntitySet( UUID entitySetId, Set<PropertyType> newPropertyTypes ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public Boolean createOrganization( Organization organization, Principal principal ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public List<Map<String, Object>> executeOrganizationKeywordSearch( String searchTerm, Set<Principal> principals ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public Boolean updateOrganization( UUID id, Optional<String> optionalTitle, Optional<String> optionalDescription ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public Boolean deleteOrganization( UUID organizationId ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public Boolean updateOrganizationPermissions(
            UUID organizationId,
            Principal principal,
            Set<Permission> permissions ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override public Boolean createEntityData(
            UUID entitySetId, String entityId, Map<UUID, String> propertyValues ) {
        return null;
    }

    @Override public List<Map<String, Object>> executeEntitySetDataSearch(
            UUID entitySetId, String searchTerm, Set<UUID> authorizedPropertyTypes ) {
        return null;
    }

    @Override
    public Boolean createEntityData( UUID entitySetId, String entityId, Map<UUID, String> propertyValues ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

    @Override
    public List<Map<String, Object>> executeEntitySetDataSearch(
            UUID entitySetId,
            String searchTerm,
            Set<UUID> authorizedPropertyTypes ) {
        throw new NotImplementedException(
                "You are trying to invoke ConductorSparkApi from somehwere else other than conductor." );
    }

}