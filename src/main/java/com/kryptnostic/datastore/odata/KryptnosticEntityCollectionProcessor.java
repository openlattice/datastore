package com.kryptnostic.datastore.odata;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.olingo.commons.api.data.ContextURL;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.EntityCollection;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.ex.ODataRuntimeException;
import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.commons.api.http.HttpHeader;
import org.apache.olingo.commons.api.http.HttpStatusCode;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.EntityCollectionProcessor;
import org.apache.olingo.server.api.serializer.EntityCollectionSerializerOptions;
import org.apache.olingo.server.api.serializer.ODataSerializer;
import org.apache.olingo.server.api.serializer.SerializerResult;
import org.apache.olingo.server.api.uri.UriInfo;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourceEntitySet;

import com.kryptnostic.datastore.Datastore;
import com.kryptnostic.datastore.services.EntityStorageClient;

public class KryptnosticEntityCollectionProcessor implements EntityCollectionProcessor {
    private OData                     odata;
    private ServiceMetadata           serviceMetadata;
    private final EntityStorageClient storage;

    public KryptnosticEntityCollectionProcessor( EntityStorageClient storage ) {
        this.storage = storage;
    }

    @Override
    public void init( OData odata, ServiceMetadata serviceMetadata ) {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }

    @Override
    public void readEntityCollection(
            ODataRequest request,
            ODataResponse response,
            UriInfo uriInfo,
            ContentType responseFormat ) throws ODataApplicationException, ODataLibraryException {
        // 1st we have retrieve the requested EntitySet from the uriInfo object (representation of the parsed service
        // URI)
        List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
        UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get( 0 ); // in our example,
                                                                                                   // the first segment
                                                                                                   // is the EntitySet

        EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();

        // 2nd: fetch the data from backend for this requested EntitySetName
        // it has to be delivered as EntitySet object
        EntityCollection entitySet = storage.readEntitySetData( edmEntitySet );

        // 3rd: create a serializer based on the requested format (json)
        ODataSerializer serializer = odata.createSerializer( responseFormat );

        // 4th: Now serialize the content: transform from the EntitySet object to InputStream
        EdmEntityType edmEntityType = edmEntitySet.getEntityType();
        ContextURL contextUrl = ContextURL.with().entitySet( edmEntitySet ).build();

        final String id = request.getRawBaseUri() + "/" + edmEntitySet.getName();
        EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with().id( id )
                .contextURL( contextUrl ).build();

        SerializerResult serializerResult = serializer.entityCollection( serviceMetadata,
                edmEntityType,
                entitySet,
                opts );
        InputStream serializedContent = serializerResult.getContent();

        // Finally: configure the response object: set the body, headers and status code
        response.setContent( serializedContent );
        response.setStatusCode( HttpStatusCode.OK.getStatusCode() );
        response.setHeader( HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString() );
    }


}
