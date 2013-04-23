package syndeticlogic.catena.text;

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

import syndeticlogic.catena.text.IdTable.TableType;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;

public class DictionaryReader {
	private final static Log log = LogFactory.getLog(InvertedFileReader.class);
	private static int BLOCK_SIZE=1048576; // 256k
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
	
	public void loadDictionary(Map<Integer, String> idToWord, List<InvertedListDescriptor> descriptors) throws IOException {
        int blockSize = (int) Math.min((long) BLOCK_SIZE, channel.size());
        byte[] block = new byte[blockSize];
        int header = Type.LONG.length()+Type.BYTE.length()+Type.INTEGER.length();
        buffer.get(block, 0, Type.LONG.length()+Type.BYTE.length()+Type.INTEGER.length());
        long docDictionaryLength = Codec.getCodec().decodeLong(block, 0);
        byte coding = Codec.getCodec().decodeByte(block, Type.LONG.length());
        int prefixLength = Codec.getCodec().decodeInteger(block, Type.LONG.length()+Type.BYTE.length());
        
        if(coding == 1) {
            InvertedList.setTableType(TableType.VariableByteCoded);
        }
        
        DocumentDescriptor docDesc = new DocumentDescriptor();
        long cursor = header;
        while (buffer.hasRemaining()) {
            if (blockSize != Math.min(buffer.remaining(), blockSize)) {
                blockSize = Math.min(buffer.remaining(), blockSize);
                block = new byte[blockSize];
            }
            buffer.get(block, 0, blockSize);
            int blockOffset = 0;

            HashMap<Integer, String> idToPrefix = new HashMap<Integer, String>();
            while (blockOffset < blockSize) {
                int size=0;
                if(cursor < prefixLength) {
                    int prefixId = Codec.getCodec().decodeInteger(block, blockOffset);
                    String prefix = Codec.getCodec().decodeString(block, blockOffset+Type.INTEGER.length());
                    size = Type.INTEGER.length()+Type.STRING.length()+prefix.length();
                    idToPrefix.put(prefixId, prefix);
                } else if(cursor < docDictionaryLength) {
                    size = docDesc.decode(block, blockOffset);
                    idToWord.put(docDesc.getDocId(), idToPrefix.get(docDesc.getDocPrefixId())+File.separator+docDesc.getDoc());//idToWord.put(docDesc.getWordId(), docDesc.getWord());
                } else {
                    InvertedListDescriptor listDesc = new InvertedListDescriptor(null, -1, -1, -1);
                    size = listDesc.decode(block, blockOffset);
                    descriptors.add(listDesc);
                }
                cursor += size;
                blockOffset += size;                
            }
        }
	}
	
    public int getBlockSize() {
        return BLOCK_SIZE;
    }
    
    public void setBlockSize(int pageSize) {
        BLOCK_SIZE = pageSize;
    }
}
