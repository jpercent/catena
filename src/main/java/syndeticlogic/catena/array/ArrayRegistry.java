package syndeticlogic.catena.array;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.CompositeKey;
import syndeticlogic.catena.utility.PropertiesUtility;

public class ArrayRegistry {
    private static final Log log = LogFactory.getLog(ArrayRegistry.class);
	private HashMap<CompositeKey, ArrayDescriptor> arrays;
	private int splitThreshold;
	
	public ArrayRegistry(Properties config) {
	    arrays = new HashMap<CompositeKey, ArrayDescriptor>();
		String baseDirectoryPath = config.getProperty(PropertiesUtility.CONFIG_BASE_DIRECTORY);
		splitThreshold = new Integer(config.getProperty(PropertiesUtility.SPLIT_THRESHOLD)).intValue();
		createBaseDirectory(baseDirectoryPath); 
		loadDescriptors(baseDirectoryPath);
	}

    public Collection<ArrayDescriptor> arrayDescriptors() {
        return arrays.values();
    }
	
	public void createArray(CompositeKey id, Type type) {

	    if(arrays.containsKey(id)) {
	        log.error("creating array failed; an array with id = "+id.toString()+" already exits");
			throw new RuntimeException("Array already exists");
		}
		
		ArrayDescriptor arrayDesc = new ArrayDescriptor(id, type, splitThreshold);
		ArrayDescriptor.createSegment(arrayDesc);
		arrays.put(arrayDesc.id(), arrayDesc);
	}

	public void deleteArray(CompositeKey id) {
		if(!arrays.containsKey(id)) {
			throw new RuntimeException("Dipshit, you cannot delete an array that does not exist");
		}
		arrays.remove(id);
		// XXX - need to clean up persistent resources
		throw new RuntimeException("need to cleanup persistent resources");
	}

	public Array createArrayInstance(CompositeKey id) {
		ArrayDescriptor arrayDesc = arrays.get(id);
		if(arrayDesc == null) {
			throw new RuntimeException("Array does not exist");
		}
		SegmentController sctrl = new SegmentController(arrayDesc);
		return new Array(arrayDesc, sctrl);
	}

	public ArrayDescriptor arrayDescriptor(CompositeKey id) {
		if(!arrays.containsKey(id)) {
			throw new RuntimeException("ArrayDescriptor does not exist");
		}
		return arrays.get(id);
	}
	
	private void loadDescriptors(String baseDirectoryPath) {
        Loader loader = new Loader();
        List<ArrayDescriptor> arrayList = loader.load(baseDirectoryPath);
        
        for(ArrayDescriptor arrayDesc : arrayList) {
            assert arrayDesc != null;
            if(!arrayDesc.checkIntegrity()) {
                throw new RuntimeException("WTF - meta data and actual data do not match");
            }
            
            if(arrayDesc.length() == 0) {
                ArrayDescriptor.createSegment(arrayDesc);
            }
            arrays.put(arrayDesc.id(), arrayDesc);
        }
	}
	
	private void createBaseDirectory(String baseDirectoryPath) {
	   File baseDirectory = new File(baseDirectoryPath);
       if(!baseDirectory.exists()) {
           try {
               FileUtils.forceMkdir(baseDirectory);
           } catch (IOException e) {
               e.printStackTrace();
               throw new RuntimeException(e);
           }
       }
       assert baseDirectory.isDirectory();
	}
}
