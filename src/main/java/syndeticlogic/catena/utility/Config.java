package syndeticlogic.catena.utility;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.array.Array;
import syndeticlogic.catena.array.ArrayRegistry;
import syndeticlogic.catena.array.BinaryArray;
import syndeticlogic.catena.store.PageFactory;
import syndeticlogic.catena.store.PageFactory.BufferPoolMemoryType;
import syndeticlogic.catena.store.PageFactory.CachingPolicy;
import syndeticlogic.catena.store.PageFactory.PageDescriptorType;
import syndeticlogic.catena.store.PageManager;
import syndeticlogic.catena.store.SegmentManager;
import syndeticlogic.catena.store.SegmentManager.CompressionType;

public class Config {
   
    private final Log log = LogFactory.getLog(Array.class);
    private final static int DEFAULT_CHUNK_SIZE = 1048576;
    
    public Properties p = new Properties();
    public static String prefix = "target" + File.separator + "arrayTest" + File.separator;
    public CompositeKey key; 
    public PageFactory pf;
    public PageManager pm;
    public ArrayRegistry arrayRegistry;
    public BinaryArray array;
    public int retryLimit = 2;
    
    public void configure(CompressionType compressionStrategy, 
                          String prefix, 
                          BufferPoolMemoryType memoryType, 
                          CachingPolicy cachingPolicy, 
                          PageDescriptorType pageDescriptorType, 
                          int retryLimit,
                          List<String> files,
                          int pageSize, double percentageOfPhysicalMemory) throws IOException 
    {
     
        com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean)
                java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        long physicalMemorySize = os.getTotalPhysicalMemorySize();
        System.out.println(" memorySize = "+physicalMemorySize);
        int pages = (int)(physicalMemorySize * percentageOfPhysicalMemory)/pageSize;
        System.out.println("pages == "+pages + " memory allocated = "+pages*pageSize);
        
        p.setProperty(PropertiesUtility.CONFIG_BASE_DIRECTORY, prefix);
        p.setProperty(PropertiesUtility.SPLIT_THRESHOLD, Integer.toString(DEFAULT_CHUNK_SIZE));

        try {
            FileUtils.forceDelete(new File(prefix));
        } catch (Exception e) {
        }

        try {
            FileUtils.forceMkdir(new File(prefix));
        } catch (IOException e) {
            String fatalMessage = "FATAL EXCEPTION -- Catena cannot create directories in "+prefix+"; creating directories and files is required";
            System.err.println(fatalMessage);
            System.out.println(fatalMessage);
            log.fatal(fatalMessage);
            return;
        }

        Codec.configureCodec(null);

        pf = new PageFactory(memoryType, //PageFactory.BufferPoolMemoryType.Java, 
                cachingPolicy,//PageFactory.CachingPolicy.PinnableLru, 
                pageDescriptorType, // PageFactory.PageDescriptorType.Unsynchronized, 
                retryLimit);

        pm = pf.createPageManager(files, pageSize, pages);
        SegmentManager.configureSegmentManager(compressionStrategy, //CompressionType.Null
                pm);        
        arrayRegistry = new ArrayRegistry(p);
    }
}
