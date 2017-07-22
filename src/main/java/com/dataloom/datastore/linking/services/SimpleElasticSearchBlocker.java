package com.dataloom.datastore.linking.services;

import com.dataloom.data.EntityKey;
import com.dataloom.data.storage.CassandraEntityDatastore;
import com.dataloom.data.storage.EntityBytes;
import com.dataloom.datastore.services.SearchService;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.linking.Entity;
import com.dataloom.linking.components.Blocker;
import com.dataloom.linking.util.UnorderedPair;
import com.dataloom.streams.StreamUtil;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.kryptnostic.datastore.services.EdmManager;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleElasticSearchBlocker implements Blocker {
    private static final Logger logger = LoggerFactory.getLogger( SimpleElasticSearchBlocker.class );

    private final EdmManager               dms;
    private final CassandraEntityDatastore dataManager;
    private       SearchService            searchService;

    private SetMultimap<UUID, UUID> linkIndexedByEntitySets;
    private Map<UUID, UUID>         linkingEntitySetsWithSyncId;
    private Set<UUID>               linkingES;

    // Number of search results taken in each block.
    private int     blockSize = 50;
    // Whether explanation for search results is stored.
    private boolean explain   = false;

    public SimpleElasticSearchBlocker(
            EdmManager dms,
            CassandraEntityDatastore dataManager,
            SearchService searchService ) {
        this.dataManager = dataManager;
        this.dms = dms;
        this.searchService = searchService;
    }

    @Override
    public void setLinking(
            Map<UUID, UUID> linkingEntitySetsWithSyncId,
            SetMultimap<UUID, UUID> linkIndexedByPropertyTypes,
            SetMultimap<UUID, UUID> linkIndexedByEntitySets ) {
        this.linkIndexedByEntitySets = linkIndexedByEntitySets;
        this.linkingEntitySetsWithSyncId = linkingEntitySetsWithSyncId;
        this.linkingES = new HashSet<>( linkingEntitySetsWithSyncId.keySet() );
    }

    @Override
    public Stream<UnorderedPair<Entity>> block() {
        return loadEntitySets();
    }

    private Stream<UnorderedPair<Entity>> loadEntitySets() {
        return linkingES.stream().flatMap( this::loadEntitySet );
    }

    private Stream<UnorderedPair<Entity>> loadEntitySet( UUID entitySetId ) {
        Set<UUID> propertiesSet = ImmutableSet.copyOf( linkIndexedByEntitySets.get( entitySetId ) );
        Map<UUID, PropertyType> propertiesMap = propertiesSet.stream()
                .collect( Collectors.toMap( ptId -> ptId, ptId -> dms.getPropertyType( ptId ) ) );

        Stream<Pair<EntityKey,SetMultimap<UUID,Object>>> entityKeys = dataManager
                .getEntityKeysForEntitySet( entitySetId, linkingEntitySetsWithSyncId.get( entitySetId ) )
                .map( key -> dataManager.asyncLoadEntity( key.getEntitySetId(),
                        key.getEntityId(),
                        key.getSyncId(),
                        propertiesSet ) )
                .map( StreamUtil::safeGet )
                .map( eb -> Pair.of( eb.getKey(), dataManager.rowToEntityIndexedById( eb, propertiesMap ) ) );


        return entityKeys.flatMap( entityKeyDataPair -> {
            Map<UUID, Set<String>> properties = propertiesSet.stream()
                    .collect( Collectors.toMap( ptId -> ptId,
                            ptId -> entityKeyDataPair.getValue().get( ptId ).stream()
                                    // TODO fix the uber terrible toString later
                                    .map( obj -> obj.toString() )
                                    .collect( Collectors.toSet() ) ) );

            Map<String, Object> propertiesIndexedByString = properties.entrySet().stream().collect(
                    Collectors.toMap( entry -> entry.getKey().toString(), entry -> entry.getValue() ) );

            // Blocking step: fire off query to elasticsearch.
            List<Entity> queryResults = searchService
                    .executeEntitySetDataSearchAcrossIndices( linkingEntitySetsWithSyncId,
                            properties,
                            blockSize,
                            explain );

            // return pairs of entities.
            Entity currentEntity = new Entity( entityKeyDataPair.getKey(), propertiesIndexedByString );
            return queryResults.stream().map( entity -> new UnorderedPair<Entity>( currentEntity, entity ) );
        } );
    }
}
