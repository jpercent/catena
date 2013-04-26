package syndeticlogic.catena.text.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.text.io.ReadCursor.BlockDescriptor;
import syndeticlogic.catena.text.postings.InvertedListDescriptor;

public class BlockReader {
	private final static Log log = LogFactory.getLog(InvertedFileReader.class);
	private static int BLOCK_SIZE=10*1048576;
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
        BlockDescriptor blockDesc = new BlockDescriptor();
        blockDesc.buf = new byte[(int) Math.min(this.blockSize, channel.size())];
        boolean headerDecoded = false;
        
        while (buffer.hasRemaining()) {
            buffer.get(blockDesc.buf, 0, blockDesc.buf.length);            
            if(!headerDecoded) {
                cursor.decodeHeader(blockDesc);
                headerDecoded = true;
                if(blockDesc.offset < blockDesc.buf.length) {
                    cursor.decodeBlock(blockDesc);
                }
            } else {
                cursor.decodeBlock(blockDesc);
            }
            blockDesc.offset = 0;
            if(buffer.remaining() < blockDesc.buf.length) {
                blockDesc.buf = new byte[buffer.remaining()];
            } else {
                for(int i = 0; i < blockDesc.buf.length; i++) {
                    blockDesc.buf[i] = 0;
                }
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

    public static void printBinary(byte[] buffer2, int offset, int ret) {
        for (int i =0 ; i < ret;i++) {
            System.out.print(buffer2[offset+i]+", ");
        }
        System.out.println();

        
    }

    public void scanBlock(ReadCursor cursor) {
        BlockDescriptor blockDesc = new BlockDescriptor();
        blockDesc.buf = new byte[(int) Math.min(this.blockSize, buffer.remaining())];
        buffer.get(blockDesc.buf, 0, blockDesc.buf.length);            
        cursor.decodeBlock(blockDesc);
    }
}
