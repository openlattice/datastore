package com.kryptnostic.datastore.exceptions;

import java.util.List;

import com.kryptnostic.datastore.util.ErrorDTO;
import com.kryptnostic.datastore.util.ErrorsDTO;

public class BatchExceptions extends RuntimeException {
    private static final long serialVersionUID = 9049360916124505696L;

    private ErrorsDTO         errors           = new ErrorsDTO();

    public BatchExceptions( ErrorsDTO errors ) {
        this.errors = errors;
    }

    public BatchExceptions( List<ErrorDTO> list ) {
        this.errors.setErrors( list );
    }

    public ErrorsDTO getErrors() {
        return errors;
    }

    public void addError( String type, String message ) {
        errors.addError( type, message );
    }

    public void addError( String type, String object, String message ) {
        errors.addError( type, object, message );
    }

    public void setErrors( ErrorsDTO errors ) {
        this.errors = errors;
    }

    public void setErrors( List<ErrorDTO> list ) {
        this.errors.setErrors( list );
    }
}
