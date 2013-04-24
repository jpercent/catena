package syndeticlogic.catena.text.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.text.DocumentDescriptor;
import syndeticlogic.catena.text.postings.InvertedList;
import syndeticlogic.catena.text.postings.InvertedListDescriptor;
import syndeticlogic.catena.text.postings.IdTable.TableType;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;

public class BlockReader {
	private final static Log log = LogFactory.getLog(InvertedFileReader.class);
	private static int BLOCK_SIZE=60*1048576; // 256k
	private FileInputStream inputStream;
	private FileChannel channel;
	private File file;
	private MappedByteBuffer buffer;
	private boolean closed = true;
	private int blockSize = BLOCK_SIZE;
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
	
	public void read(ReadCursor cursor) throws IOException {
        int blockSize = (int) Math.min(this.blockSize, channel.size());
        byte[] block = new byte[blockSize];
        boolean headerDecoded = false;
        int blockOffset = 0;
        
        while (buffer.hasRemaining()) {
            if (buffer.remaining() < blockSize) {
                blockSize = buffer.remaining();
                block = new byte[blockSize];
                blockOffset = 0;
            }            
            buffer.get(block, 0, blockSize);
            if(!headerDecoded) {
                blockOffset += cursor.decodeHeader(block, blockOffset);
                headerDecoded = true;
            } else {
                assert blockOffset <= blockSize;
                blockOffset += cursor.decodeBlock(block, blockOffset);
            }
        }
	}
    
    public int getBlockSize() {
        return blockSize;
    }
    
    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }
    
    public static int getDefaultBlockSize() {
        return BLOCK_SIZE;
    }
    
    public static void setDefaultBlockSize(int blockSize) {
        BLOCK_SIZE = blockSize;
    }
}
