package com.kryptnostic.datastore.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus( value = HttpStatus.NOT_FOUND )
public class ResourceNotFoundException extends RuntimeException {
    private static final long serialVersionUID = 9036215896394849079L;

    public ResourceNotFoundException(String msg) {
        super(msg);
    }
}
