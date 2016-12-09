package com.kryptnostic.datastore.util;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ErrorsDTO {
    private List<ErrorDTO> errors = new ArrayList<ErrorDTO>();

    public void addError( String type, String message ) {
        errors.add( new ErrorDTO( type, message ) );
    }

    public void addError( String type, String object, String message ) {
        errors.add( new ErrorDTO( type, object, message ) );
    }

    public List<ErrorDTO> getErrors() {
        return errors;
    }

    public void setErrors( List<ErrorDTO> errors ) {
        this.errors = errors;
    }

    @Override
    public String toString() {
        return "MultipleValidationErrorsDTO [errors=" + errors + "]";
    }

    @JsonIgnore
    public boolean isEmpty() {
        return errors.isEmpty();
    }
}
