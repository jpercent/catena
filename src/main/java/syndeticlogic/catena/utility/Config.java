package syndeticlogic.catena.utility;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.array.Array;
import syndeticlogic.catena.array.ArrayRegistry;
import syndeticlogic.catena.store.PageFactory;
import syndeticlogic.catena.store.PageFactory.BufferPoolMemoryType;
import syndeticlogic.catena.store.PageFactory.CachingPolicy;
import syndeticlogic.catena.store.PageFactory.PageDescriptorType;
import syndeticlogic.catena.store.PageManager;
import syndeticlogic.catena.store.SegmentManager;
import syndeticlogic.catena.store.SegmentManager.CompressionType;
import syndeticlogic.catena.type.ValueFactory;

public class Config {
   
    private final Log log = LogFactory.getLog(Array.class);
    private final static int DEFAULT_CHUNK_SIZE = 1048576;
    private Properties properties;
    private CompressionType compressionType;
    private BufferPoolMemoryType memoryType;
    private CachingPolicy cachingPolicy;
    private PageDescriptorType pageType;
    private DynamicClassLoader classLoader;
    private DynamicProperties dynamicProperties;
    private ValueFactory valueFactory;
    private PageFactory pageFactory;
    private PageManager pageManager;
    private ArrayRegistry arrayRegistry;
    private String prefix;
    private double memoryPercentage;
    private long physicalMemorySize;
    private int retryLimit;
    private int pageSize;
    private boolean truncate;
    private int pages;

    public Config() {
    	loadProperties();
        configurePrefix(prefix, truncate);
        configureCodec();
        configureArraySystem();
    }
   
    private void configureArraySystem() {
        com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean)
                java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        this.physicalMemorySize = os.getTotalPhysicalMemorySize();
        this.pages = (int)(physicalMemorySize * memoryPercentage)/pageSize;
        
        pageFactory = new PageFactory(memoryType, cachingPolicy, pageType, retryLimit);
        pageManager = pageFactory.createPageManager(null, pageSize, pages);
        SegmentManager.configureSegmentManager(compressionType, pageManager);        
        arrayRegistry = new ArrayRegistry(properties);
    }
    
    private void loadProperties() {    	
        try {
			properties = PropertiesUtility.load("catena.properties");
		} catch (Exception e) {
			log.fatal("could not load catena.properties"+e, e);
			throw new RuntimeException(e);
		}
        compressionType = CompressionType.valueOf((String) properties.get("compression_type"));
        memoryType = BufferPoolMemoryType.valueOf((String) properties.get("memory_type"));
        cachingPolicy = CachingPolicy.valueOf((String) properties.get("caching_policy"));
        pageType = PageDescriptorType.valueOf((String) properties.get("page_type"));
        prefix = (String) properties.get("prefix");
        memoryPercentage = Double.parseDouble((String) properties.get("physical_memory_percentage"));
        assert memoryPercentage < 1;
        
        retryLimit = Integer.parseInt((String)properties.get("retry_limit"));
        pageSize = Integer.parseInt((String)properties.get("page_size"));
        truncate = Boolean.parseBoolean((String)properties.get("truncate"));
        properties.setProperty(PropertiesUtility.CONFIG_BASE_DIRECTORY, prefix);
        properties.setProperty(PropertiesUtility.SPLIT_THRESHOLD, Integer.toString(DEFAULT_CHUNK_SIZE));
    }
   
    private void configurePrefix(String prefix, boolean truncate) {
        if(truncate) {
        	try {
        		FileUtils.forceDelete(new File(prefix));
        	} catch (Exception e) {
        	}
        }

    	File prefixFile = new File(prefix);
    	boolean creationSuccess = true;
    	if(!prefixFile.exists()) {
    		try {
    			FileUtils.forceMkdir(prefixFile);
    		} catch (IOException e) {
    			creationSuccess = false;
    		}
    	}
    	
    	if(!(prefixFile.exists() && prefixFile.canRead() && prefixFile.canWrite() && creationSuccess)) {
    		String fatalMessage = "Catena must have read/write permissions on "+prefixFile.getAbsolutePath();
    		log.fatal(fatalMessage);
    		throw new RuntimeException(fatalMessage);
    	}            		    	
    }
    
    private void configureCodec() {
        String classesDirectory = FilenameUtils.concat(prefix, "classes");
        String propertiesDirectory = FilenameUtils.concat(prefix, "properties");
    	try {
            File classesDirectoryFile = new File(classesDirectory);
            File propertiesDirectoryFile = new File(propertiesDirectory);
    		FileUtils.forceMkdir(classesDirectoryFile);
    		FileUtils.forceMkdir(propertiesDirectoryFile);
    	} catch (IOException e) {
    		String fatalMessage = "cannot create directories in "+prefix;
    		log.warn(fatalMessage);
    	}
        classLoader = new DynamicClassLoader(classesDirectory);
        dynamicProperties = new DynamicProperties(propertiesDirectory); 
        valueFactory = new ValueFactory(dynamicProperties, classLoader);        
        Codec.configureCodec(valueFactory);
    }

	public Properties getProperties() {
		return properties;
	}

	public BufferPoolMemoryType getMemoryType() {
		return memoryType;
	}

	public CachingPolicy getCachingPolicy() {
		return cachingPolicy;
	}

	public ValueFactory getValueFactory() {
		return valueFactory;
	}

	public PageManager getPageManager() {
		return pageManager;
	}

	public ArrayRegistry getArrayRegistry() {
		return arrayRegistry;
	}

	public String getPrefix() {
		return prefix;
	}

	public double getMemoryPercentage() {
		return memoryPercentage;
	}

	public long getPhysicalMemorySize() {
		return physicalMemorySize;
	}
}
