package syndeticlogic.catena.type;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.utility.DynamicProperties;
import syndeticlogic.catena.utility.SimpleNotificationListener;

public class ValueFactory implements SimpleNotificationListener {
    private final Log log = LogFactory.getLog(ValueFactory.class);
    private final HashMap<String, String> types = new HashMap<String, String>();
    private final DynamicProperties dynamicProperties;
    
    public ValueFactory(DynamicProperties properties) {
        dynamicProperties = properties;
        updateTypes();
    }
    
    public Value getValue(String valueType) {
        synchronized (this) {
            try {
                String valueClass = types.get(valueType).toString();
                Class<?> clazz = Class.forName(valueClass);
                return (Value) clazz.newInstance();
            } catch (Exception e) {
                log.error("Excpetion creating class " + valueType + ": " + e, e);
            }
        }
        return null;
    }

    public void updateTypes() {
        Set<Entry<Object, Object>> props = dynamicProperties.properties().entrySet();
        for(Entry<Object, Object> prop : props) {
            if(!(prop.getKey() instanceof java.lang.String && prop.getValue() instanceof java.lang.String)) {
                continue;
            }
            String clazz = (String) prop.getKey();
            String qualifiedClass = (String) prop.getValue();
            types.put(clazz, qualifiedClass);
        }
    }
    
    @Override
    public void notified() {
        updateTypes();
    }
}
 
    
