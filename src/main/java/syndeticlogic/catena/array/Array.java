package syndeticlogic.catena.array;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.array.BinaryArray.LockType;
import syndeticlogic.catena.type.Value;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.CompositeKey;
import syndeticlogic.catena.utility.Config;

public class Array<T extends Value> {
	private static final Log log = LogFactory.getLog(Array.class);
	private static Config config;
    private BinaryArray array;
    
    public Array(BinaryArray backingArray) {
        array = backingArray;
    }
	
    public void position(int position, LockType lockType) {
        array.position(position, lockType);
    }
    
	public T[] scan() {
	    return null;
	}
	
	public void append(T t) {
	    array.append(t.data(), t.offset(), t.length());
	}
	
	public void commit() {
	    array.commit();
	}
	
	public void complete(LockType lockType) {
	    array.complete(lockType);
	}
	
	@SuppressWarnings("rawtypes")
	public static Array<?> create(String name, Type t) {
		configure();
		String masterKey = config.getPrefix()+File.separator+name+File.separator;
		CompositeKey key = createKey(masterKey);
		try {
	        return new Array(config.getArrayRegistry().createArrayInstance(key));
		} catch(RuntimeException e) {
			config.getArrayRegistry().createArray(key, t);
			return new Array(config.getArrayRegistry().createArrayInstance(key));
		}
	}
	
	@SuppressWarnings("rawtypes")
	public static Array<?> createAndTruncateIfExists(String name, Type t) {
		configure();
		String masterKey = config.getPrefix()+File.separator+name+File.separator;
		CompositeKey key = createKey(masterKey);
		truncate(masterKey);
		config.getArrayRegistry().createArray(key, t);
		return new Array(config.getArrayRegistry().createArrayInstance(key));
	}
	
	public static void configure() {
		synchronized (Array.class) {
			if (config == null) {
				config = new Config();
			}
		}
	}

	public static void truncate(String path) {
		try {
			FileUtils.forceDelete(new File(path));
		} catch (IOException e) {
			log.debug("truncation failed "+e, e);
		}		
	}

	public static CompositeKey createKey(String master) {
		CompositeKey key = new CompositeKey();
		key.append(master);
		return key;
	}

	public void appendBulk(byte[] jvm, int offset, int i) {
	    array.append(jvm, offset, i);
	}
}
