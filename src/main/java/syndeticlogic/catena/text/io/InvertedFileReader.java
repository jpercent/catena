package syndeticlogic.catena.text.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.text.postings.InvertedList;
import syndeticlogic.catena.text.postings.InvertedListDescriptor;
import syndeticlogic.catena.type.Type;

public class InvertedFileReader {
	private final static Log log = LogFactory.getLog(InvertedFileReader.class);
	private static int BLOCK_SIZE=500*1048576;
	private FileInputStream inputStream;
	private FileChannel channel;
	private File file;
	private MappedByteBuffer buffer;
	private boolean closed = true;
	
	public void open(String fileName) {
	    file = new File(fileName);
		assert file.exists();
		try {
			inputStream = new FileInputStream(file);
			channel = inputStream.getChannel();
			buffer = channel.map(MapMode.READ_ONLY, 0L, channel.size());
			closed = false;
		} catch(IOException e) {
			log.fatal("Could not tokenize "+file.getAbsolutePath()+": "+e);
			throw new RuntimeException(e);
		} 
	}
	
	public void close() {
	    if (closed) return;
		try {
			inputStream.close();
			channel.close();
			inputStream = null;
			channel = null;
			file = null;
			buffer = null;
		} catch(Exception e) {
			log.warn("failed to clean up"+e, e);
		} finally {
		    closed = true;
		}
	}
	
	public void scanFile(HashMap<Integer, String> idToWord, TreeMap<String, InvertedList> postings) throws IOException {
        int blockSize = (int) Math.min((long) BLOCK_SIZE, channel.size());
        byte[] block = new byte[blockSize];

        while (buffer.hasRemaining()) {
            if (blockSize != Math.min(buffer.remaining(), blockSize)) {
                blockSize = Math.min(buffer.remaining(), blockSize);
                block = new byte[blockSize];
            }
            // XXX - if the invertedList for an object is > BLOCK_SIZE we will never read it.
            buffer.get(block, 0, blockSize);
            int offset = decodePostings(block, blockSize, postings, idToWord);
            buffer.position((buffer.position() - (buffer.position() - offset)));
        }
	}

	public int scanBlock(int start, List<InvertedListDescriptor> descriptors, HashMap<Integer, String> idToWord, TreeMap<String, InvertedList> postings) {
	    InvertedListDescriptor cursor = descriptors.get(start);
	    assert buffer.remaining() > cursor.getLength();
        int blockSize = (int) Math.min((long) BLOCK_SIZE, buffer.remaining());
        blockSize = (int)Math.max(blockSize, cursor.getLength());
        byte[] block = new byte[blockSize];

        int sizeCursor = blockSize;
        assert sizeCursor >= cursor.getLength();
        while(true) {
            sizeCursor -= cursor.getLength();
            if(sizeCursor > 0) {
                start++;
                cursor = descriptors.get(start);
            } else {
                break;
            }
        }
        // XXX - if the invertedList for an object is > BLOCK_SIZE we will never read it.
        buffer.get(block, 0, blockSize);
        int offset = decodePostings(block, blockSize, postings, idToWord);
        buffer.position((buffer.position() - (buffer.position() - offset)));
        return start;
	}
	
    public InvertedList scanEntry(InvertedListDescriptor desc) throws IOException {
        buffer = channel.map(MapMode.READ_ONLY, desc.getOffset(), desc.getLength());
        byte[] copy = new byte[desc.getLength()];
        buffer.get(copy);
        InvertedList ret = InvertedList.create();
        ret.decode(copy, 0);
        ret.setWord(desc.getWord());
        return ret;
    }
	
	public int decodePostings(byte[] block, int blockSize, TreeMap<String, InvertedList> postings, HashMap<Integer, String> idToWord) {
	    boolean success = true;
	    int offset = 0;
	    int count = 0;
	    do {
	    	assert blockSize >= offset;
	    	if(blockSize == offset || blockSize - offset < Type.INTEGER.length()) {
	    		break;
	    	}
	    	try {
	    		InvertedList list = InvertedList.create();
	    		list.decode(block, offset);
	    		offset += list.size();
	    		list.setWord(idToWord.get(list.getWordId()));
	    		assert !postings.containsKey(list.getWord());
	    		postings.put(list.getWord(), list);
	    		count++;
	    	} catch(Throwable t) {
	    		t.printStackTrace();
	    		success = false;
	    	}
	    	if(count < 1) {
	    		throw new RuntimeException("Attempted to scan an inverted list that larger than "+BLOCK_SIZE
	    				+"; this program does not support that at this time");
	    	}
	    } while(success);
		return offset;
	}
	
    public int getBlockSize() {
        return BLOCK_SIZE;
    }
    
    public void setBlockSize(int pageSize) {
        BLOCK_SIZE = pageSize;
    }
}
