package syndeticlogic.catena.type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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
    private Integer nextOrdinal;
    
    public ValueFactory(DynamicProperties properties, DynamicClassLoader loader) {
        this.dynamicProperties = properties;
        this.dynamicLoader = loader;
        this.systemTypes = new HashMap<String, String>();
        this.systemTypes.put("syndeticlogic.catena.type.TypeValue", "0");
        this.systemTypes.put("syndeticlogic.catena.type.BooleanValue", "1");
        this.systemTypes.put("syndeticlogic.catena.type.ByteValue", "2");
        this.systemTypes.put("syndeticlogic.catena.type.CharValue", "3");
        this.systemTypes.put("syndeticlogic.catena.type.ShortValue", "4");
        this.systemTypes.put("syndeticlogic.catena.type.IntegerValue", "5");
        this.systemTypes.put("syndeticlogic.catena.type.LongValue", "6");
        this.systemTypes.put("syndeticlogic.catena.type.FloatValue", "7");
        this.systemTypes.put("syndeticlogic.catena.type.DoubleValue", "8");
        this.systemTypes.put("syndeticlogic.catena.type.String", "9");
        this.systemTypes.put("syndeticlogic.catena.type.BinaryValue", "10");
        this.systemTypes.put("syndeticlogic.catena.type.CodeableValue", "11");
        nextOrdinal = new Integer("12");
        this.userDefinedTypes = new HashMap<String, String>();
        this.dynamicProperties.addSimpleNotificationListener(this);
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
            log.warn(valueType+" is undefined in system and user defined type lists");
        }
        return null;
    }
    
    @SuppressWarnings("unchecked")
    public synchronized Map.Entry<String, String>[] getTypes() {
        int types = userDefinedTypes.size();
        types += systemTypes.size();
        ArrayList<Map.Entry<String, String>> typeDescriptors = new ArrayList<Map.Entry<String, String>>(types);
        for(Map.Entry<String, String> type : systemTypes.entrySet()) {
            typeDescriptors.add(type);
        }
        for(Map.Entry<String, String> type : userDefinedTypes.entrySet()) {
            typeDescriptors.add(type);
        }
        return (Map.Entry<String, String>[])typeDescriptors.toArray();
    }
    
    protected synchronized void updateTypes() {
        Set<Map.Entry<Object, Object>> props = dynamicProperties.properties().entrySet();
        for(Map.Entry<Object, Object> prop : props) {
            if(!(prop.getKey() instanceof java.lang.String && prop.getValue() instanceof java.lang.String)) {
                continue;
            }
            String qualifiedClass = (String) prop.getKey();
            String ordinal = (String) prop.getValue();
            userDefinedTypes.put(qualifiedClass, ordinal);
            try {
                dynamicLoader.loader().loadClass(qualifiedClass);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        
    }
    
    protected synchronized String claimNextOrdinal() {
        String currentOrdinal = nextOrdinal.toString();
        nextOrdinal = new Integer(nextOrdinal.intValue() + 1);
        return currentOrdinal;
    }
    
    @Override
    public void notified() {
        updateTypes();
    }
}
