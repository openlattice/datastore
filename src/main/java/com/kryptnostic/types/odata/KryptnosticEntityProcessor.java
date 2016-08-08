package com.kryptnostic.types.odata;

import org.apache.olingo.commons.api.format.ContentType;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.ODataLibraryException;
import org.apache.olingo.server.api.ODataRequest;
import org.apache.olingo.server.api.ODataResponse;
import org.apache.olingo.server.api.ServiceMetadata;
import org.apache.olingo.server.api.processor.EntityProcessor;
import org.apache.olingo.server.api.uri.UriInfo;

public class KryptnosticEntityProcessor implements EntityProcessor {
    private OData           odata;
    private ServiceMetadata serviceMetadata;

    @Override
    public void init( OData odata, ServiceMetadata serviceMetadata ) {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
    }

    @Override
    public void readEntity( ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat )
            throws ODataApplicationException, ODataLibraryException {
        // TODO Auto-generated method stub

    }

    @Override
    public void createEntity(
            ODataRequest request,
            ODataResponse response,
            UriInfo uriInfo,
            ContentType requestFormat,
            ContentType responseFormat ) throws ODataApplicationException, ODataLibraryException {
        // TODO Auto-generated method stub

    }

    @Override
    public void updateEntity(
            ODataRequest request,
            ODataResponse response,
            UriInfo uriInfo,
            ContentType requestFormat,
            ContentType responseFormat ) throws ODataApplicationException, ODataLibraryException {
        // TODO Auto-generated method stub

    }

    @Override
    public void deleteEntity( ODataRequest request, ODataResponse response, UriInfo uriInfo )
            throws ODataApplicationException, ODataLibraryException {
        // TODO Auto-generated method stub

    }

}
