package com.kryptnostic.datastore.util;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import com.kryptnostic.datastore.exceptions.BatchExceptions;
import com.kryptnostic.datastore.exceptions.ForbiddenException;
import com.kryptnostic.datastore.exceptions.ResourceNotFoundException;

/**
 * Created by yao on 9/20/16.
 */
@ControllerAdvice
public class DataStoreExceptionHandler {
    private static final Logger logger    = LoggerFactory.getLogger( DataStoreExceptionHandler.class );
    private static final String ERROR_MSG = "";

    @ExceptionHandler( { NullPointerException.class, ResourceNotFoundException.class } )
    public ResponseEntity<ErrorDTO> handleNotFoundException( Exception e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<ErrorDTO>(
                new ErrorDTO( e.getClass().getName(), e.getMessage() ),
                HttpStatus.NOT_FOUND );
    }

    @ExceptionHandler( IllegalArgumentException.class )
    public ResponseEntity<ErrorDTO> handleIllegalArgumentException( Exception e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<ErrorDTO>(
                new ErrorDTO( e.getClass().getName(), e.getMessage() ),
                HttpStatus.BAD_REQUEST );
    }

    @ExceptionHandler( MethodArgumentNotValidException.class )
    public ResponseEntity<ErrorsDTO> handleMethodArgumentNotValidException( MethodArgumentNotValidException e ) {
        logger.error( ERROR_MSG, e );
        ErrorsDTO dto = processErrors( e.getBindingResult().getAllErrors() );
        return new ResponseEntity<ErrorsDTO>( dto, HttpStatus.BAD_REQUEST );
    }

    private ErrorsDTO processErrors( List<ObjectError> errors ) {
        ErrorsDTO dto = new ErrorsDTO();

        for ( ObjectError error : errors ) {
            dto.addError( error.getObjectName(), error.getDefaultMessage() );
        }

        return dto;
    }

    @ExceptionHandler( IllegalStateException.class )
    public ResponseEntity<ErrorDTO> handleIllegalStateException( Exception e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<ErrorDTO>(
                new ErrorDTO( e.getClass().getName(), e.getMessage() ),
                HttpStatus.INTERNAL_SERVER_ERROR );
    }

    @ExceptionHandler( BatchExceptions.class )
    public ResponseEntity<ErrorsDTO> handleBatchExceptions( BatchExceptions e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<ErrorsDTO>( e.getErrors(), e.getStatusCode() );
    }

    @ExceptionHandler( ForbiddenException.class )
    public ResponseEntity<ErrorDTO> handleUnauthorizedExceptions( ForbiddenException e ) {
        logger.error( ERROR_MSG, e );
        return new ResponseEntity<ErrorDTO>(
                new ErrorDTO( e.getClass().getName(), e.getMessage() ),
                HttpStatus.UNAUTHORIZED );
    }
}
