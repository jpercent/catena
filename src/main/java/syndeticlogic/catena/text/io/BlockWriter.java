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
	int blockSize=BLOCK_SIZE;
	boolean closed=true;
	
	public void open(String fileName) {
		try {
			dictionaryFile = new File(fileName);
			dictionaryFile.delete();
			assert dictionaryFile.createNewFile();
			outputStream = new FileOutputStream(dictionaryFile);
			channel = outputStream.getChannel();
			direct = ByteBuffer.allocateDirect(blockSize);
			closed = false;
			headerSize = 0;
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
	
    public long writeFile(WriteCursor cursor) {
        long fileOffset = 0;
        try {
            headerSize = cursor.reserveHeaderLength();
            channel.position(headerSize);
            
            fileOffset = headerSize;

            byte[] jvmBuffer = new byte[blockSize];
            int offset = 0;
            while (cursor.hasNext()) {
                int length = cursor.nextLength();
                if (offset + length >= blockSize) {
                    write(jvmBuffer, 0, offset);
                    offset = 0;
                    //continue;
                }
//               / System.out.println(" length "+length+ " offset "+offset);
                int encoded = cursor.encodeNext(jvmBuffer, offset);
                assert encoded == length;
                offset += encoded;
                fileOffset += encoded;
            }
            write(jvmBuffer, 0 , offset);
            channel.position(0);
            int encodedHeaderSize = cursor.encodeHeader(jvmBuffer, 0);
            assert encodedHeaderSize == headerSize;
            write(jvmBuffer, 0, headerSize);
        } catch (Throwable t) {
            log.fatal("exception writing index file " + t, t);
            throw new RuntimeException(t);
        }
        return fileOffset;
    }
    
    public long writeBlock(WriteCursor cursor) {
        try {
//            fileOffset = headerSize;

            byte[] jvmBuffer = new byte[blockSize];
            int offset = 0;
            while (cursor.hasNext()) {
                int length = cursor.nextLength();
                if (offset + length >= blockSize) {
                    write(jvmBuffer, 0, offset);
                    offset = 0;
                    //continue;
                }
//               / System.out.println(" length "+length+ " offset "+offset);
                int encoded = cursor.encodeNext(jvmBuffer, offset);
                assert encoded == length;
                offset += encoded;
  //              fileOffset += encoded;
            }
            write(jvmBuffer, 0 , offset);
            //channel.position(0);
          //  int encodedHeaderSize = cursor.encodeHeader(jvmBuffer, 0);
           // assert encodedHeaderSize == headerSize;
           // write(jvmBuffer, 0, headerSize);
        } catch (Throwable t) {
            log.fatal("exception writing index file " + t, t);
            throw new RuntimeException(t);
        }
        return -1 ;//fileOffset;
    }
    
    private void write(byte[] jvmBuffer, int offset, int length) throws IOException {
        System.err.println("jvmlenght "+jvmBuffer.length+" offset "+offset+" length "+length);
        System.err.println("buffer "+direct.capacity()+" limit "+direct.limit()+" position "+direct.position());
        direct.put(jvmBuffer, 0, length);
        direct.rewind();
        direct.limit(length);
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
