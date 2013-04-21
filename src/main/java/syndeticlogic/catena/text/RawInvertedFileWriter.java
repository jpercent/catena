package syndeticlogic.catena.text;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.SortedMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RawInvertedFileWriter implements InvertedFileWriter {
	private static final Log log = LogFactory.getLog(RawInvertedFileWriter.class);
	private static int BLOCK_SIZE = 1048576/2/2;	
	FileOutputStream outputStream=null;
	FileChannel channel=null;
	File indexFile;
	ByteBuffer direct;
	boolean closed=true;
	
    @Override
	public void open(String fileName) {
		try {
			indexFile = new File(fileName);
			indexFile.delete();
			assert indexFile.createNewFile();
			outputStream = new FileOutputStream(indexFile);
			channel = outputStream.getChannel();
			direct = ByteBuffer.allocateDirect(BLOCK_SIZE);
			closed = false;
		} catch(Throwable e) {
			log.fatal("could not open index file "+fileName+" for writing "+e, e);
			throw new RuntimeException(e);
		}
	}
    
    @Override
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
    		indexFile = null;
		} catch (IOException e) {
			log.fatal("exception cleaning up"+e, e);
		}finally {
		    closed = true;
		}
	}
	
    @Override
    public long writeFile(SortedMap<String, InvertedList> postings, List<InvertedListDescriptor> invertedListDescriptors) {
        long fileOffset = 0;
        try {
            byte[] jvm = new byte[BLOCK_SIZE];
            int offset = 0;
            for (InvertedList list : postings.values()) {
                int length = list.size();
                if (offset + length >= BLOCK_SIZE) {
                    direct.put(jvm, 0, offset);
                    direct.rewind();
                    direct.limit(offset);
                    channel.write(direct);
                    direct.rewind();
                    direct.limit(direct.capacity());
                    offset = 0;
                }
                int written = list.encode(jvm, offset);
                assert written == length;
                invertedListDescriptors.add(new InvertedListDescriptor(list.getWord(), fileOffset, length, list.getDocumentFrequency()));
                offset += length;
                fileOffset += length;
            }
            direct.put(jvm, 0, offset);
            direct.rewind();
            direct.limit(offset);
            channel.write(direct);
            direct.rewind();
            direct.limit(direct.capacity());
        } catch (Throwable t) {
            log.fatal("exception writing index file " + t, t);
            throw new RuntimeException(t);
        }
        return fileOffset;
    }
    
    @Override
    public int getBlockSize() {
        return BLOCK_SIZE;
    }
    
    @Override
    public void setBlockSize(int blockSize) {
        BLOCK_SIZE = blockSize;
    }
}
