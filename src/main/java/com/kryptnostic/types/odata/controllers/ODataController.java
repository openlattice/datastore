package com.kryptnostic.types.odata.controllers;

import java.util.ArrayList;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.olingo.commons.api.edmx.EdmxReference;
import org.apache.olingo.server.api.OData;
import org.apache.olingo.server.api.ODataHttpHandler;
import org.apache.olingo.server.api.ServiceMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.datastore.odata.KryptnosticEdmProvider;
import com.kryptnostic.datastore.odata.KryptnosticEntityCollectionProcessor;
import com.kryptnostic.datastore.odata.KryptnosticEntityProcessor;
import com.kryptnostic.datastore.services.DatasourceManager;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.datastore.services.EntityStorageClient;

@Controller
public class ODataController {
    private static final Logger logger = LoggerFactory.getLogger( ODataController.class );
    @Inject
    private HazelcastInstance   hazelcast;

    @Inject
    private EdmManager          dms;
    
    @Inject
    private EntityStorageClient storage;
    
    @Inject
    private DatasourceManager dsm;
    

    @RequestMapping( { "", "/*" } )
    public void handleOData( HttpServletRequest req, HttpServletResponse resp ) throws ServletException {
        try {
            // create odata handler and configure it with CsdlEdmProvider and Processor
            OData odata = OData.newInstance();
            ServiceMetadata edm = odata.createServiceMetadata( new KryptnosticEdmProvider( dms ),
                    new ArrayList<EdmxReference>() );
            ODataHttpHandler handler = odata.createHandler( edm );
            handler.register( new KryptnosticEntityCollectionProcessor() );
            handler.register( new KryptnosticEntityProcessor( storage,dsm ) );
            // let the handler do the work
            handler.process( req, resp );
        } catch ( RuntimeException e ) {
            logger.error( "Server Error occurred in ExampleServlet", e );
            throw new ServletException( e );
        }
    }
}
