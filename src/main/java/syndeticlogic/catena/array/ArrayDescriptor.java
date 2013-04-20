package syndeticlogic.catena.array;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.CodeHelper;
import syndeticlogic.catena.utility.Codec;
import syndeticlogic.catena.utility.CompositeKey;
import syndeticlogic.catena.utility.ThreadSafe;

import syndeticlogic.catena.store.Segment;
import syndeticlogic.catena.store.SegmentManager;

@ThreadSafe
public class ArrayDescriptor {
    public static final String ARRAY_DESC_FILE_NAME = ".arrayDesc";
    private static final Log log = LogFactory.getLog(ArrayDescriptor.class);
    private CompositeKey master;
    private int length;
    private long nextKey;
    private LinkedList<Integer> sizes;
    private long arraySize; 
    private TreeMap<CompositeKey, Segment> segments;
    private CompositeKey lastKey;
    private Segment lastSegment;
    private final FileChannel commitChannel;
    private final int splitThreshold;
    private final Type type;
    private final int typeSize;
    private final ReentrantLock lock;
    
    public static class ValueDescriptor {
        public CompositeKey segmentId;
        public int segmentOffset;
        public long byteOffset;
        public int index;
        public int valueSize;

        public ValueDescriptor() {
        }

        public ValueDescriptor(CompositeKey segmentId, int segmentOffset, long byteOffset, 
                int index, int size) {
            this.segmentId = segmentId;
            this.segmentOffset = segmentOffset;
            this.byteOffset = byteOffset;
            this.index = index;
            this.valueSize = size;
        }
    }
    
    public static byte[] encode(ArrayDescriptor arrayDesc) {
        CodeHelper coder = Codec.getCodec().coder();
        byte[] header = null;
        arrayDesc.acquire();
        try {
            int elements = 7;
            if(!arrayDesc.type.isFixedLength()) {
                elements += arrayDesc.length;
            }
            coder.append(elements);
            header = coder.encodeByteArray();
            coder.reset();
            
            String stringKey = new String(CompositeKey.encode(arrayDesc.master));
            coder.append(stringKey);
            coder.append(arrayDesc.type);
            coder.append(arrayDesc.length);
            coder.append(arrayDesc.nextKey);
            coder.append(arrayDesc.splitThreshold);
            coder.append(arrayDesc.arraySize);
            if(!arrayDesc.type.isFixedLength()) {
                for(Integer i : arrayDesc.sizes) {
                    coder.append(i);
                }
            }
        } finally {
            arrayDesc.release();
        }
        byte[] buffer = coder.encodeByteArray();

        CRC32 crc = new CRC32();
        crc.update(buffer, 0, buffer.length);
        long crc32 = crc.getValue();
        coder.append(crc32);
        int size = coder.computeSize();
        buffer = new byte[size+header.length];
        System.arraycopy(header, 0, buffer, 0, header.length);
        coder.encode(buffer, header.length);
        return buffer;
    }
    
    public static ArrayDescriptor decode(byte[] buffer, int offset) {
        assert buffer != null && offset >= 0;
        assert buffer.length > 0 && buffer.length > offset;
        CodeHelper coder = Codec.getCodec().coder();
        List<Object> members = coder.decode(buffer, offset, 1);
        int elements = ((Integer)members.get(0)).intValue();
        offset += Type.INTEGER.length() + coder.metaSize(1);
        coder.reset();

        members = coder.decode(buffer, offset, elements);
        String stringKey = (String) members.get(0);
        Type type = (Type)members.get(1);
        int length = ((Integer)members.get(2)).intValue();
        long nextKey = ((Long)members.get(3)).longValue();
        int splitThreshold = ((Integer)members.get(4)).intValue();
        long arraySize = ((Long)members.get(5)).longValue();
        
        LinkedList<Integer> sizes = null;
        int current = 6;
        if(!type.isFixedLength()) {
            sizes = new LinkedList<Integer>();
            for(; current < length+6; current++) {
                sizes.add(((Integer)members.get(current)));
            }
        }
        long crc32 = ((Long)members.get(current)).longValue();
        
        coder.reset();
        coder.append(stringKey);
        coder.append(type);
        coder.append(length);
        coder.append(nextKey);
        coder.append(splitThreshold);
        coder.append(arraySize);
        if(!type.isFixedLength()) {
            for(Integer i : sizes) {
                coder.append(i);
            }
        }
        
        byte[] test = coder.encodeByteArray();
        CRC32 crc = new CRC32();
        crc.update(test, 0, test.length);
        long crc32_1 = crc.getValue();
        assert crc32 == crc32_1;

        CompositeKey master = CompositeKey.decode(stringKey.getBytes());
        ArrayDescriptor ret = new ArrayDescriptor(master, type, splitThreshold);
        ret.length = length;
        ret.arraySize = arraySize;
        ret.sizes = sizes;
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
    }      
   
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
        this.lock = new ReentrantLock();

        try {
            String path = master.toString();
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
        
        arraySize = 0;
        if(type.isFixedLength()) {
            typeSize = type.length();
        } else {
            typeSize = -1;
            sizes = new LinkedList<Integer>();
        }
	}
	
	public boolean checkIntegrity() {
        boolean ret = true;
        long size = 0;
        ValueDescriptor valueDescriptor = null;

        for(int index=0; index < length; index++) {
            valueDescriptor = find(index);
	        if(valueDescriptor == null) {
	            log.error("checkIntegrity failed - less elements than expected");
	            ret = false;
	            break;
	        }
	        
	        size += valueDescriptor.valueSize;
	        if(index+1 == length) {
	            valueDescriptor = find(index+1);
	            if(valueDescriptor != null) {
	                log.error("checkIntegrity failed - more elements than expected ");//index ="+index+" length = "+length);
	                ret = false;
	            }
	        }
	    }
	    
	    if(ret && size != size()) {
	        log.error("checkIntegrity failed - size mismatch");
	        ret = false;
	    }
	    return ret;
	}
	
	public int convertIndexToOffset(int index) {
		if (index > length) {
			throw new RuntimeException("index out of range");
		}
		int ret = index * typeSize;
        if (!isFixedLength()) {
        	ret = 0;
        	int i = 0;
        	for(Integer size : sizes) {
        		if (!(i < index)) { 
        			break;
        		}
        		ret += size.intValue();
        		assert ret > 0;
        		i++;
        	}
        }
        return ret;
	}
	
    public void addSegment(CompositeKey key, Segment s) {
        segments.put(key, s);
        if(lastKey == null || key.compareTo(lastKey) > 0) {
            lastSegment = s;
            lastKey = key;
        }
    }

    public ValueDescriptor find(int index) {
        if(index >= length) {
            return null;
        }
        
        ValueDescriptor valueDescriptor = configureValue(index);
        long current = 0;
        long previousEnd = 0;
        // XXX segment lock here?
        acquire();
        try {
            for (Entry<CompositeKey, Segment> iter : segments.entrySet()) {
                Segment segment = iter.getValue();
                previousEnd = current;
                current += segment.size();
                if (current > valueDescriptor.byteOffset) {
                    int offset = (int) (valueDescriptor.byteOffset - previousEnd);
                    valueDescriptor.segmentId = iter.getKey();
                    valueDescriptor.segmentOffset = offset;
                }
            }
        } finally {
            release();
        }
        return valueDescriptor;
    }
    
    public int valueSize(int index) {
        if(index >= length) {
            return -1;
        } else if(typeSize != -1) {
            return typeSize;
        } else {
            return sizes.get(index);
        }
    }
    
    private ValueDescriptor configureValue(int index) {
        if(typeSize != -1) {
            return  new ValueDescriptor(null, -1, index * typeSize, index, typeSize);
        }
        
        ValueDescriptor v = new ValueDescriptor();
        v.index = index;

        long pos = 0;
        Iterator<Integer> i = sizes.iterator();
        Integer value = null;

        do {
            assert i.hasNext();
            // throw new
            // RuntimeException("Index given is out of range of what is actually stored; index = "+index+" pos = "+pos);
            value = i.next();
            v.byteOffset += value.intValue();
            pos++;
        } while (pos <= index);
        assert value != null;
        v.valueSize = value.intValue();
        assert v.valueSize == sizes.get(index).intValue();
        return v;
    }
    
    public int update(int index, int size) {
        int ret = typeSize;
        if(typeSize == -1) {
            arraySize -= sizes.get(index).intValue();
            arraySize += size;
            ret = sizes.set(index, new Integer(size)).intValue();            
        }
        return ret;

    }

    public void append(int size) {
        assert lastSegment != null;
        if(lastSegment.size() + size > splitThreshold) {
            ArrayDescriptor.createSegment(this);
        }
                
        if(typeSize == -1) {
            sizes.add(new Integer(size));
        } else {
            assert size == typeSize;
        }
        arraySize += size;
        length++;
    }

    public int delete(int index) {
        length--;
        int ret = typeSize;
        if(typeSize == -1) {
            ret = sizes.remove(index).intValue();
        }
        arraySize -= ret;
        return ret;
    }
    
    public void persist() {
        byte[] serialized = ArrayDescriptor.encode(this);
        try {
            commitChannel.position(0);
            commitChannel.truncate(serialized.length);
            commitChannel.write(ByteBuffer.wrap(serialized));
            commitChannel.force(false);
            //valueIndex.persist();
        } catch (IOException e) {
            String msg = " could not sync array identified by "+master.toString();
            log.error(msg, e);
            throw new RuntimeException(msg);
        }
    }

    public void acquire() {
        lock.lock();
    }
    
    public void release() {
        lock.unlock();
    }
    
	public CompositeKey id() {
		return master;
	}

	public CompositeKey nextId() {
	    CompositeKey key = new CompositeKey();
        key.append(master);
        key.append(nextKey);
	    nextKey++;
	    return key;
	}
	
	public boolean isFixedLength() {
	    return type.isFixedLength();
	}
	
	public int typeSize() {
	    return typeSize;
	}
	
	public Type type() {
		return type;
	}
	
	public int length() {
		return length;
	}
	
	public long size() {
	    return arraySize;
	}
	
    public Collection<Segment> segments() {
        return segments.values();
    }
}
