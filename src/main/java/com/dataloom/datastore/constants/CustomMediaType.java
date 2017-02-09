package com.dataloom.datastore.constants;

import java.io.Serializable;

import org.springframework.http.MediaType;

public class CustomMediaType implements Serializable {

    private static final long     serialVersionUID = -948015479363478194L;

    public static final MediaType TEXT_CSV;

    public static final String    TEXT_CSV_VALUE   = "text/csv;charset=UTF-8";
    
    public static final MediaType TEXT_YAML;
    
    public static final String    TEXT_YAML_VALUE  = "text/x-yaml;charset=UTF-8";

    static {
        TEXT_CSV = MediaType.valueOf( TEXT_CSV_VALUE );
        TEXT_YAML = MediaType.valueOf( TEXT_YAML_VALUE );
    }
}
