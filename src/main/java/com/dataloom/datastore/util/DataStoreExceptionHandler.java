package com.dataloom.datastore.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.dataloom.authorization.ForbiddenException;
import com.dataloom.edm.exceptions.TypeExistsException;
import com.kryptnostic.datastore.exceptions.BatchException;
import com.kryptnostic.datastore.exceptions.ResourceNotFoundException;
import com.kryptnostic.datastore.util.ErrorsDTO;

@RestControllerAdvice
public class DataStoreExceptionHandler {
    private static final Logger logger    = LoggerFactory.getLogger( DataStoreExceptionHandler.class );
    private static final String ERROR_MSG = "";

    @ExceptionHandler( { NullPointerException.class, ResourceNotFoundException.class } )
    public ResponseEntity<ErrorsDTO> handleNotFoundException( Exception e ) {
        logger.error( ERROR_MSG, e );
        if( e.getMessage() != null ){
            return new ResponseEntity<ErrorsDTO>(
                    new ErrorsDTO( "ResourceNotFoundException", e.getMessage() ),
                    HttpStatus.NOT_FOUND );
        }
        return new ResponseEntity<ErrorsDTO>( HttpStatus.NOT_FOUND );
    }

    @ExceptionHandler( { IllegalArgumentException.class, HttpMessageNotReadableException.class } )
    public ResponseEntity<ErrorsDTO> handleIllegalArgumentException( Exception e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<ErrorsDTO>(
                new ErrorsDTO( "IllegalArgumentException", e.getMessage() ),
                HttpStatus.BAD_REQUEST );
    }

    @ExceptionHandler( IllegalStateException.class )
    public ResponseEntity<ErrorsDTO> handleIllegalStateException( Exception e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<ErrorsDTO>(
                new ErrorsDTO( "IllegalStateException", e.getMessage() ),
                HttpStatus.INTERNAL_SERVER_ERROR );
    }

    @ExceptionHandler( TypeExistsException.class )
    public ResponseEntity<ErrorsDTO> handleTypeExistsException( Exception e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<ErrorsDTO>(
                new ErrorsDTO( "TypeExistsException", e.getMessage() ),
                HttpStatus.CONFLICT );
    }

    @ExceptionHandler( ForbiddenException.class )
    public ResponseEntity<ErrorsDTO> handleUnauthorizedExceptions( ForbiddenException e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<ErrorsDTO>(
                new ErrorsDTO( "ForbiddenException", e.getMessage() ),
                HttpStatus.UNAUTHORIZED );
    }
    
    @ExceptionHandler( BatchException.class )
    public ResponseEntity<ErrorsDTO> handleBatchExceptions( BatchException e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<ErrorsDTO>( e.getErrors(), e.getStatusCode() );
    }
    
    @ExceptionHandler( Exception.class )
    public ResponseEntity<ErrorsDTO> handleOtherExceptions( Exception e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<ErrorsDTO>(
                new ErrorsDTO( e.getClass().getSimpleName(), e.getMessage() ),
                HttpStatus.INTERNAL_SERVER_ERROR );
    }
}
