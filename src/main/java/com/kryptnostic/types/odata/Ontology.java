package com.kryptnostic.types.odata;

import java.util.List;
import java.util.Map;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;

/**
 * This class repre
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt; 
 *
 */
public class Ontology {
    public static class EntitySchema {
        private final Map<String, EdmPrimitiveTypeKind> properties;
        private final List<String> keyProperties;
        public EntitySchema( Map<String, EdmPrimitiveTypeKind> properties , List<String> keyProperties ) {
            this.properties = properties;
            this.keyProperties = keyProperties;
        }
        
        public Map<String, EdmPrimitiveTypeKind> getProperties() {
            return properties;
        }
        
        public List<String> getKeyProperties() {
            return keyProperties;
        }
    }
}
