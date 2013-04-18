package syndeticlogic.catena.array;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.array.BinaryArray.LockType;
import syndeticlogic.catena.type.Value;
import syndeticlogic.catena.utility.CompositeKey;

public class Array<T extends Value> {
    private final Log log = LogFactory.getLog(Array.class);
    CompositeKey key; 
    BinaryArray array;
    int retryLimit = 2;
    
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
}
