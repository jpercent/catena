package syndeticlogic.catena.array;

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.predicate.Predicate;
import syndeticlogic.catena.type.Value;
import syndeticlogic.catena.utility.Transaction;

public class Array<Type extends Value> {
    private final Log log = LogFactory.getLog(Array.class);
    private final static int DEFAULT_CHUNK_SIZE = 1048576;
	private BinaryArray binaryArray;
	private Predicate predicate;
	private Transaction transaction;
	private ByteBuffer chunk;
	
	Array(BinaryArray array, Transaction t, Predicate p) {
		this.binaryArray = array;
		this.transaction = t;
		this.predicate = p;
		this.chunk = ByteBuffer.allocate(DEFAULT_CHUNK_SIZE);
	}
	
	public void setPredicate(Predicate predicate) {
	    this.predicate = predicate;
	}
	
	Type[] scan() {
	    int size = binaryArray.remaining();
	    return null;
	    //binaryArray.
	}
	
	void update(Predicate p) {
	    
	}
	
	void delete() {
	    
	}
	
	void append(byte[] bytes) {
	    
	}
}
