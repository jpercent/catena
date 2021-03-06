package syndeticlogic.catena.utility;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileChangeEvent;

public class DynamicClassLoader extends FileSystemWatcher {
    private static final Log log = LogFactory.getLog(DynamicClassLoader.class);
    private volatile URLClassLoader loader;
    private Set<URL> jarFiles;
    private Set<SimpleNotificationListener> listeners;
    
    public DynamicClassLoader(String directory) {
        super(directory);
        loader = new URLClassLoader(new URL[0]);
        jarFiles = new HashSet<URL>();
        listeners = new HashSet<SimpleNotificationListener>();
    }

    @Override
    public void fileCreated(FileChangeEvent created) throws Exception {
        updateJars(created);
    }

    public void updateJars(FileChangeEvent event) {
        try {
            URL url = event.getFile().getURL();
            synchronized (this) {
                jarFiles.add(url);
                loader = createNewLoader();
                for(SimpleNotificationListener listener : listeners) {
                    listener.notified();
                }
            }
        } catch (Exception e) {
            log.error("Exception on file changed event " + e, e);
        }
    }
    
    protected URLClassLoader createNewLoader() {
        URL[] urls = new URL[jarFiles.size()];
        Iterator<URL> urlIter = jarFiles.iterator();
        for(int i = 0; i < jarFiles.size(); i++) {
            urls[i] = urlIter.next();
        }
        return new URLClassLoader(urls);
    }
    
    public ClassLoader loader() {
        synchronized(this) {
            return loader;
        }
    }
    
    public void addSimpleNotificationListener(SimpleNotificationListener listener) {
        synchronized(this) {        
            listeners.add(listener);
        }
    }
}
