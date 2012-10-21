package syndeticlogic.catena.utility;

import java.net.URL;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileChangeEvent;

public class DynamicProperties extends FileSystemWatcher {
    private static final Log log = LogFactory.getLog(DynamicProperties.class); 
    private Set<SimpleNotificationListener> listeners;
    private volatile Properties properties;
    private Set<URL> files;
    
    public DynamicProperties(Properties baseProperties, String directory) {
        super(directory);
        this.properties = baseProperties;
        this.listeners = new HashSet<SimpleNotificationListener>();
        this.files = new HashSet<URL>();
    }
    
    public DynamicProperties(String directory) {
        this(new Properties(), directory);
    }

    @Override
    public void fileChanged(FileChangeEvent changed) throws Exception {
        updateFiles(changed);
    }

    @Override
    public void fileCreated(FileChangeEvent created) throws Exception {
        updateFiles(created);
    }
    
    public void updateFiles(FileChangeEvent event) {
        try {
            URL url = event.getFile().getURL();

            Properties newProps = PropertiesUtility.load(event.getFile().getURL());
            if (newProps == null) {
                return;
            }

            Set<Entry<Object, Object>> props = newProps.entrySet();
            for (Entry<Object, Object> prop : props) {
                properties.put(prop.getKey(), prop.getValue());
            }
            synchronized (this) {
                files.add(url);
                for(SimpleNotificationListener listener : listeners) {
                    listener.notified();
                }
            }
        } catch (Exception e) {
            log.error("Exception on file changed event " + e, e);
        }
    }
    
    public Properties properties() {
        return properties;
    }
    
    public URL[] files() {
        synchronized(this) {
            return (URL[])files.toArray();
        }
    }
    
    public void addSimpleNotificationListener(SimpleNotificationListener listener) {
        synchronized(this) {
            listeners.add(listener);
        }
    }
}
