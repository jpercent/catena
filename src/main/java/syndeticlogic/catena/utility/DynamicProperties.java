package syndeticlogic.catena.utility;

import java.net.URL;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileChangeEvent;

public class DynamicProperties extends FileSystemWatcher {
    private static final Log log = LogFactory.getLog(DynamicProperties.class); 
    private Set<URL> files;
    volatile Properties properties;
    
    public DynamicProperties(Properties baseProperties, String directory) {
        this(directory);
        this.properties = baseProperties;
    }
    
    public DynamicProperties(String directory) {
        super(directory);
        this.properties = new Properties();
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
            synchronized (this) {
                files.add(url);
            }
            Properties newProps = PropertiesUtility.load(event.getFile().getURL());
            if (newProps == null) {
                return;
            }

            Set<Entry<Object, Object>> props = newProps.entrySet();
            for (Entry<Object, Object> prop : props) {
                properties.put(prop.getKey(), prop.getValue());
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
}
