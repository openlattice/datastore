package com.kryptnostic.datastore.services;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collector;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.Futures;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.datastore.Datastore;

public class EntityStorageClient {
    private static final Logger         logger = LoggerFactory
            .getLogger( EntityStorageClient.class );
    // private final IMap<String, FullQualifiedName> entitySets;
    // private final IMap<FullQualifiedName, EntitySchema> entitySchemas;
    private final EdmManager            dms;
    private final CassandraTableManager tableManager;
    private final Session               session;
    private final String                keyspace;

    public EntityStorageClient(
            String keyspace,
            HazelcastInstance hazelcast,
            EdmManager dms,
            Session session,
            CassandraTableManager tableManager,
            CassandraStorage storage,
            MappingManager mm ) {
        this.dms = dms;
        this.tableManager = tableManager;
        this.session = session;
        this.keyspace = keyspace;
    }

    public EntityCollection readEntitySetData( EdmEntitySet edmEntitySet ) throws ODataApplicationException {
        // exec.submit( new ConductorCallable )
        EntityCollection productsCollection = new EntityCollection();
        // check for which EdmEntitySet the data is requested
        if ( Datastore.ES_PRODUCTS_NAME.equals( edmEntitySet.getName() ) ) {
            List<Entity> productList = productsCollection.getEntities();

            // add some sample product entities
            final Entity e1 = new Entity()
                    .addProperty( new Property( null, "ID", ValueType.PRIMITIVE, 1 ) )
                    .addProperty( new Property( null, "Name", ValueType.PRIMITIVE, "Notebook Basic 15" ) )
                    .addProperty( new Property(
                            null,
                            "Description",
                            ValueType.PRIMITIVE,
                            "Notebook Basic, 1.7GHz - 15 XGA - 1024MB DDR2 SDRAM - 40GB" ) );
            e1.setId( createId( "Products", 1 ) );
            productList.add( e1 );

            final Entity e2 = new Entity()
                    .addProperty( new Property( null, "ID", ValueType.PRIMITIVE, 2 ) )
                    .addProperty( new Property( null, "Name", ValueType.PRIMITIVE, "1UMTS PDA" ) )
                    .addProperty( new Property(
                            null,
                            "Description",
                            ValueType.PRIMITIVE,
                            "Ultrafast 3G UMTS/HSDPA Pocket PC, supports GSM network" ) );
            e2.setId( createId( "Products", 1 ) );
            productList.add( e2 );

            final Entity e3 = new Entity()
                    .addProperty( new Property( null, "ID", ValueType.PRIMITIVE, 3 ) )
                    .addProperty( new Property( null, "Name", ValueType.PRIMITIVE, "Ergo Screen" ) )
                    .addProperty( new Property(
                            null,
                            "Description",
                            ValueType.PRIMITIVE,
                            "19 Optimum Resolution 1024 x 768 @ 85Hz, resolution 1280 x 960" ) );
            e3.setId( createId( "Products", 1 ) );
            productList.add( e3 );
        } else {
            // TODO: RPC to Spark to load data.
        }
        return productsCollection;
    }

    public Entity readEntityData( EdmEntitySet edmEntitySet, List<UriParameter> keyParams )
            throws ODataApplicationException {

        EdmEntityType edmEntityType = edmEntitySet.getEntityType();
        // readEntityData( edmEntityType.getFullQualifiedName() );

        for ( UriParameter keyParam : keyParams ) {
            String name = keyParam.getName();
            String text = keyParam.getText();
            EdmProperty edmProperty = (EdmProperty) edmEntityType.getProperty( name );
        }
        // actually, this is only required if we have more than one Entity Type
        // if(edmEntityType.getName().equals(DemoEdmProvider.ET_PRODUCT_NAME)){
        //// return getProduct(edmEntityType, keyParams);
        // }

        return null;
    }

    // public Entity readEntityData( Map<FullQualifiedName, Object> key ) {
    //
    // }

    public Entity createEntityData( UUID aclId, UUID syncId, EdmEntitySet edmEntitySet, Entity requestEntity ) {
        FullQualifiedName entityFqn = edmEntitySet.getEntityType().getFullQualifiedName();
        Preconditions.checkArgument(
                dms.isExistingEntitySet( entityFqn, edmEntitySet.getName() ),
                "Cannot add data to non-existant entity set." );
        return createEntityData( aclId, syncId, edmEntitySet.getName(), entityFqn, requestEntity ).getRight();
    }

    public Pair<UUID, Entity> createEntityData(
            UUID aclId,
            UUID syncId,
            String entitySetName,
            FullQualifiedName entityFqn,
            Entity requestEntity ) {
        PreparedStatement query = Preconditions.checkNotNull(
                tableManager.getInsertEntityPreparedStatement( entityFqn ),
                "Insert data prepared statement does not exist." );

        // this is dangerous, but fairly common practice.
        // best way to fix is to have large pool of generated UUIDs to pull from that can be replenished in bulk.
        UUID objectId = UUID.randomUUID();
        BoundStatement boundQuery = query.bind( objectId,
                aclId,
                ImmutableSet.of( entitySetName ),
                ImmutableList.of( syncId ) );
        session.execute( boundQuery );

        EntityType entityType = dms.getEntityType( entityFqn.getNamespace(), entityFqn.getName() );
        writeProperties( entityType,
                keyspace,
                objectId,
                aclId,
                syncId,
                requestEntity.getProperties() );
        requestEntity.setId( URI.create( objectId.toString() ) );
        return Pair.of( objectId, requestEntity );
    }

    private Iterable<ResultSet> writeProperties(
            EntityType entityType,
            String keyspace,
            UUID objectId,
            UUID aclId,
            UUID syncId,
            List<Property> properties ) {
        final Set<FullQualifiedName> key = entityType.getKey();
        final Set<FullQualifiedName> propertyTypes = entityType.getProperties();
        final SetMultimap<String, FullQualifiedName> nameLookup = HashMultimap.create();

        propertyTypes.forEach( fqn -> nameLookup.put( fqn.getName(), fqn ) );
        Iterable<List<ResultSetFuture>> propertyInsertion = properties.parallelStream().map( property -> {
            FullQualifiedName fqn = resolveFqn( entityType.getFullQualifiedName(), property, nameLookup );

            if ( fqn == null ) {
                logger.error( "Invalid property {} for entity type {}... skipping", property, entityType );
                return null;
            }

            PreparedStatement insertQuery = tableManager.getUpdatePropertyPreparedStatement( fqn );

            logger.info( "Attempting to write property value: {}", property.getValue() );
            // Not safe to change this order without understanding bindMarker re-ordering quirks of QueryBuilder
            ResultSetFuture rsfInsert = session.executeAsync(
                    insertQuery.bind( ImmutableList.of( syncId ), objectId, aclId, property.getValue() ) );
            if ( key.contains( fqn ) ) {
                PreparedStatement indexQuery = tableManager.getUpdatePropertyIndexPreparedStatement( fqn );
                // Not safe to change this order without understanding bindMarker re-ordering quirks of QueryBuilder
                ResultSetFuture rsfIndex = session.executeAsync(
                        indexQuery.bind( ImmutableList.of( syncId ), property.getValue(), aclId, objectId ) );
                return Arrays.asList( rsfInsert, rsfIndex );
            }
            return Arrays.asList( rsfInsert );

        } )::iterator;

        try {
            return Futures.allAsList( Iterables.concat( propertyInsertion ) ).get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Failed writing properties for entity for object {}", objectId );
            return ImmutableList.of();
        }
    }

    private static FullQualifiedName resolveFqn(
            FullQualifiedName entityType,
            Property property,
            SetMultimap<String, FullQualifiedName> nameLookup ) {
        String propertyType = property.getType();
        // Heuristic for type being FQN string.
        if ( propertyType.contains( "." ) ) {
            return new FullQualifiedName( propertyType );
        } else {
            Set<FullQualifiedName> possibleTypes = nameLookup.get( propertyType );
            if ( possibleTypes.size() == 1 ) {
                return possibleTypes.iterator().next();
            } else {
                logger.error( "Unable to resolve property {} for entity type {}... possible properties: {}",
                        property,
                        entityType,
                        possibleTypes );
                return null;
            }
        }
    }

    private URI createId( String entitySetName, Object id ) {
        try {
            return new URI( entitySetName + "(" + String.valueOf( id ) + ")" );
        } catch ( URISyntaxException e ) {
            throw new ODataRuntimeException( "Unable to create id for entity: " + entitySetName, e );
        }
    }

}
