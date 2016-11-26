package com.kryptnostic.datastore.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus( value = HttpStatus.BAD_REQUEST )
public class BadRequestException extends RuntimeException {
    private static final long serialVersionUID = 9049360916124505696L;

    public BadRequestException( String msg ) {
        super( msg );
    }
}
