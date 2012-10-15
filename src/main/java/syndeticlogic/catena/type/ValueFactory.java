package syndeticlogic.catena.type;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.utility.DynamicClassLoader;
import syndeticlogic.catena.utility.DynamicProperties;
import syndeticlogic.catena.utility.SimpleNotificationListener;

public class ValueFactory implements SimpleNotificationListener {
    private final Log log = LogFactory.getLog(ValueFactory.class);
    private final HashMap<String, String> systemTypes;
    private final HashMap<String, String> userDefinedTypes;
    private final DynamicProperties dynamicProperties;
    private final DynamicClassLoader dynamicLoader;
    
    public ValueFactory(DynamicProperties properties, DynamicClassLoader loader, HashMap<String, String> systemTypes) {
        this.dynamicProperties = properties;
        this.dynamicLoader = loader;
        this.systemTypes = systemTypes;
        this.userDefinedTypes = new HashMap<String, String>();
        this.dynamicProperties.addPropertyEventListener(this);
        updateTypes();
    }
    
    public synchronized Value getValue(String valueType) {
        if (systemTypes.containsKey(valueType)) {
            try {
                Class<?> clazz = Class.forName(valueType);
                return (Value) clazz.newInstance();
            } catch (Exception e) {
                log.error("Excpetion creating class "+valueType+": "+e, e);
            }
        } else if (userDefinedTypes.containsKey(valueType)) {
            try {
                Class<?> clazz = Class.forName(valueType, true, dynamicLoader.loader());
                return (Value) clazz.newInstance();
            } catch (Exception e) {
                log.error("Exception error creating class "+valueType+": "+e, e);
            }
        } else {
            log.warn("Type undefined in system and user defined type lists");
        }
        return null;
    }

    public synchronized void updateTypes() {
        
        Set<Entry<Object, Object>> props = dynamicProperties.properties().entrySet();
        for(Entry<Object, Object> prop : props) {
            if(!(prop.getKey() instanceof java.lang.String && prop.getValue() instanceof java.lang.String)) {
                continue;
            }
            String qualifiedClass = (String) prop.getKey();
            String ordinal = (String) prop.getValue();
            userDefinedTypes.put(qualifiedClass, ordinal);
        }
    }
    
    @Override
    public void notified() {
        updateTypes();
    }
}
 
    
