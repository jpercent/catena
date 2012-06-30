package syndeticlogic.catena.array;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;

import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.CompositeKey;
import syndeticlogic.catena.utility.ThreadSafe;

import syndeticlogic.catena.codec.CodeHelper;
import syndeticlogic.catena.codec.Codec;
import syndeticlogic.catena.store.Segment;
import syndeticlogic.catena.store.SegmentManager;

@ThreadSafe
public class ArrayDescriptor {
    public static final String ARRAY_DESC_FILE_NAME = ".arrayDesc";
    private static final Log log = LogFactory.getLog(ArrayDescriptor.class);
    private static final int elements=6;
    
    public static byte[] encode(ArrayDescriptor arrayDesc) {
        CodeHelper coder = Codec.getCodec().coder();

        String stringKey = new String(CompositeKey.encode(arrayDesc.master));
        coder.append(stringKey);
        coder.append(arrayDesc.type);
        coder.append(arrayDesc.length);
        coder.append(arrayDesc.nextKey);
        coder.append(arrayDesc.splitThreshold);
        byte[] buffer = coder.encodeByteArray();

        CRC32 crc = new CRC32();
        crc.update(buffer, 0, buffer.length);
        long crc32 = crc.getValue();
        coder.append(crc32);
        buffer = coder.encodeByteArray();
        return buffer;
    }
    
    public static ArrayDescriptor decode(byte[] buffer, int offset) {
        CodeHelper coder = Codec.getCodec().coder();
        List<Object> members = coder.decode(buffer, offset, elements);
        
        String stringKey = (String) members.get(0);
        Type type = (Type)members.get(1);
        long length = ((Long)members.get(2)).longValue();
        long nextKey = ((Long)members.get(3)).longValue();
        int splitThreshold = ((Integer)members.get(4)).intValue();
        long crc32 = ((Long)members.get(5)).longValue();
        coder.reset();
        
        coder.append(stringKey);
        coder.append(type);
        coder.append(length);
        coder.append(nextKey);
        coder.append(splitThreshold);
        byte[] test = coder.encodeByteArray();
        CRC32 crc = new CRC32();
        crc.update(test, 0, test.length);
        long crc32_1 = crc.getValue();
        assert crc32 == crc32_1;

        CompositeKey master = CompositeKey.decode(stringKey.getBytes());
        ArrayDescriptor ret = new ArrayDescriptor(master, type, splitThreshold);
        ret.length = length;
        ret.nextKey = nextKey;
        return ret;
    }
   
    public static void createSegment(ArrayDescriptor arrayDesc) {
                
        Segment newSeg=null;
        CompositeKey nextKey=null;
        try {
            nextKey = arrayDesc.nextId();
            newSeg = SegmentManager.get().create(nextKey.toString(), arrayDesc.type());
        } catch (Exception e) {
            log.error("segmentManager failed to create a new segment", e);
            throw new RuntimeException("segmentManager failed to create a new segment");
        }
        assert newSeg != null && nextKey != null;
        arrayDesc.addSegment(nextKey, newSeg);
        log.debug("first segment of array "+arrayDesc.id().toString()+" created "); 
    }      
   
	private CompositeKey master;
    private long length;
    private long nextKey;

	private TreeMap<CompositeKey, Segment> segments;
	private CompositeKey lastKey;
	private Segment lastSegment;
	
	private final FileChannel commitChannel;
	private final ElementTable elementTable;
	private final int splitThreshold;
	private final Type type;
    private final int typeSize;
    private final boolean isFixedLength;
	private final ReentrantLock lock;

	public ArrayDescriptor(CompositeKey key, Type type, int splitThreshold) {
		assert key != null && key.toString() != null;
		this.master = key;
		this.length = 0;
		this.nextKey = 0;
		this.splitThreshold = splitThreshold;
		this.segments = new TreeMap<CompositeKey, Segment>();
		this.lastKey = null;
		this.lastSegment = null;
		
        this.type = type;
		isFixedLength = type.isFixedLength();
        lock = new ReentrantLock();

        try {
            String sep = System.getProperty("file.separator");
            String path = master.toString()+sep;
            File f = new File(path);
            if(!f.exists()) {
                FileUtils.forceMkdir(f);
            } else if(!f.isDirectory()) {
                FileUtils.forceDelete(f);
                FileUtils.forceMkdir(f);
            }
            
            f = new File(path+ARRAY_DESC_FILE_NAME);
            if(!f.exists()) {
                assert f.createNewFile();
            }
            FileOutputStream metaFlusher = new FileOutputStream(f);
            commitChannel = metaFlusher.getChannel();
        } catch (FileNotFoundException e) {
            log.error("error creating channel to flush meta data changes", e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            log.error("error creating channel to flush meta data changes", e);
            throw new RuntimeException(e);
        }
        
        if(isFixedLength) {
            typeSize = type.length();
            elementTable = new FixedLengthElementTable(segments, lock, typeSize);
        } else {
            typeSize = -1;
            elementTable = new VariableLengthElementTable(master);
        }
	}
	
	public synchronized boolean checkIntegrity() {
        boolean ret = true;
        long size = 0;
        ElementDescriptor eDesc = null;

        for(long index=0; index < length; index++) {
	        eDesc = elementTable.find(index);
	        if(eDesc == null) {
	            log.error("checkIntegrity failed - less elements than expected");
	            ret = false;
	            break;
	        }
	        
	        size += eDesc.size;
	        if(index+1 == length) {
	            eDesc = elementTable.find(index+1);
	            if(eDesc != null) {
	                log.error("checkIntegrity failed - more elements than expected");
	                ret = false;
	                // last element so there's no need to break
	            }
	        }
	    }
	    
	    if(ret && size != size()) {
	        log.error("checkIntegrity failed - size mismatch");
	        ret = false;
	    }
	    return ret;
	}
	
    public synchronized void addSegment(CompositeKey key, Segment s) {
        segments.put(key, s);
        if(lastKey == null || key.compareTo(lastKey) > 0) {
            lastSegment = s;
            lastKey = key;
        }
    }
    
	public synchronized CompositeKey id() {
		return master;
	}

	public synchronized CompositeKey nextId() {
	    CompositeKey key = new CompositeKey();
        key.append(master);
        key.append(nextKey);
	    nextKey++;
	    return key;
	}
	
	public synchronized boolean isFixedLength() {
	    return isFixedLength;
	}
	
	public synchronized int typeSize() {
	    return typeSize;
	}
	
	public synchronized Type type() {
		return type;
	}
	
	public synchronized long length() {
		return length;
	}
	
	public synchronized long size() {
	    long size = 0;
        for(Segment segment : segments.values()) {
            size += segment.size();
        }
        return size;
	}

	public synchronized int update(long index, int newSize) {
	    return elementTable.update(index, newSize);
	}
	
	public synchronized void append(int elementSize) {
	    if(lastSegment.size() + elementSize > splitThreshold) {
	        ArrayDescriptor.createSegment(this);
	    }
	    elementTable.append(elementSize);
		length++;
	}
	
	public synchronized int delete(long index) {
	    length --;
	    int size = elementTable.delete(index);
	    return size;
	}
	
    public Collection<Segment> segments() {
        return segments.values();
    }
    
    public void acquire() {
        lock.lock();
    }
    
    public void release() {
        lock.unlock();
    }
    
    public synchronized void persist() {
        byte[] serialized = ArrayDescriptor.encode(this);
        try {
            commitChannel.position(0);
            commitChannel.truncate(serialized.length);
            commitChannel.write(ByteBuffer.wrap(serialized));
            commitChannel.force(false);
            elementTable.persist();
        } catch (IOException e) {
            String msg = " could not sync array identified by "+master.toString();
            log.error(msg, e);
            throw new RuntimeException(msg);
        }
    }
}
