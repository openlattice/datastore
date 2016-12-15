package com.kryptnostic.datastore.util;

public class ErrorDTO {
    private String type;
    private String object;
    private String message;

    public ErrorDTO( String type, String message ) {
        this.type = type;
        this.message = message;
    }

    public ErrorDTO( String type, String object, String message ) {
        this.type = type;
        this.object = object;
        this.message = message;
    }

    public String getType() {
        return type;
    }

    public String getObject() {
        return object;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "ErrorDTO [type=" + type + ", object=" + object + ", message=" + message + "]";
    }

}
