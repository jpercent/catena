package syndeticlogic.catena.text.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BlockWriter {
	private static final Log log = LogFactory.getLog(BlockWriter.class);
	private static int BLOCK_SIZE = 1048576;	
	FileOutputStream outputStream=null;
	FileChannel channel=null;
	File dictionaryFile;
	ByteBuffer direct;
	int headerSize;
	int blockSize;
	boolean closed=true;
	
	public void open(String fileName) {
		try {
			dictionaryFile = new File(fileName);
			dictionaryFile.delete();
			assert dictionaryFile.createNewFile();
			outputStream = new FileOutputStream(dictionaryFile);
			channel = outputStream.getChannel();
			direct = ByteBuffer.allocateDirect(BLOCK_SIZE);
			closed = false;
			headerSize = 0;
			blockSize = 0;
		} catch(Throwable e) {
			log.fatal("could not open index file "+fileName+" for writing "+e, e);
			throw new RuntimeException(e);
		}
	}
    
	public void close() {
        if(closed)
            return;
		try {
    		channel.force(true);
    		channel.close();
    		outputStream.close();
    		direct = null;
    		channel = null;
    		outputStream = null;
    		dictionaryFile = null;
		} catch (IOException e) {
			log.fatal("exception cleaning up"+e, e);
		}finally {
		    closed = true;
		}
	}
	
    public long writeDictionary(WriteCursor feed) {
        long fileOffset = 0;
        try {
            channel.position(headerSize);
            fileOffset = headerSize;

            byte[] jvmBuffer = new byte[blockSize];
            int offset = 0;
            while (feed.hasNext()) {
                int length = feed.nextLength();
                if (offset + length >= BLOCK_SIZE) {
                    write(jvmBuffer, 0, offset);
                    offset = 0;
                }
                int encoded = feed.encodeNext(jvmBuffer, offset);
                assert encoded == length;
                offset += encoded;
                fileOffset += encoded;
            }
            write(jvmBuffer, 0 , offset);
            channel.position(0);
            feed.encodeHeader(jvmBuffer, 0);
        } catch (Throwable t) {
            log.fatal("exception writing index file " + t, t);
            throw new RuntimeException(t);
        }
        return fileOffset;
    }
    
    private void write(byte[] jvmBuffer, int offset, int length) throws IOException {
        direct.put(jvmBuffer, 0, offset);
        direct.rewind();
        direct.limit(offset);
        channel.write(direct);
        direct.rewind();
        direct.limit(direct.capacity());        
    }
    
    public int getBlockSize() {
        return blockSize;
    }
    
    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }
    
    public int getHeaderSize() {
        return this.headerSize;
    }
    
    public void setHeaderSize(int headerSize) {
        this.headerSize = headerSize;
    } 
}
