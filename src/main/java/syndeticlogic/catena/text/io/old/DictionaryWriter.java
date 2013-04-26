package syndeticlogic.catena.text.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

public class DictionaryWriter {
	private static final Log log = LogFactory.getLog(DictionaryWriter.class);
	private static int BLOCK_SIZE = 1048576;	
	FileOutputStream outputStream=null;
	FileChannel channel=null;
	File dictionaryFile;
	ByteBuffer direct;
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
	
    public long writeDictionary(Map<Integer, String> idToDoc, Map<String, Integer> prefixes, List<InvertedListDescriptor> invertedListDescriptors) {
        long fileOffset = 0;
        DocumentDescriptor doc = new DocumentDescriptor();
        int count = 0;
        try {
            int header = Type.BYTE.length()+Type.LONG.length();
            channel.position(header);
            fileOffset = header;
                    
            byte[] jvm = new byte[BLOCK_SIZE];
            int offset = 0;
            for (Map.Entry<Integer, String> document : idToDoc.entrySet()) {
                doc.setDocId(document.getKey());
                doc.setDoc(document.getValue());
                int length = doc.size();
                if (offset + length >= BLOCK_SIZE) {
                    direct.put(jvm, 0, offset);
                    direct.rewind();
                    direct.limit(offset);
                    channel.write(direct);
                    direct.rewind();
                    direct.limit(direct.capacity());
                    offset = 0;
                }
                int written = doc.encode(jvm, offset);
                assert written == length;
                offset += length;
                fileOffset += length;
                count ++;
            }
            direct.put(jvm, 0, offset);
            direct.rewind();
            direct.limit(offset);
            channel.write(direct);
            direct.rewind();
            direct.limit(direct.capacity());
            
            long docIdOffset = fileOffset;
            offset = 0;
            for (InvertedListDescriptor desc : invertedListDescriptors) {
                int length = desc.size();
                if (offset + length >= BLOCK_SIZE) {
                    direct.put(jvm, 0, offset);
                    direct.rewind();
                    direct.limit(offset);
                    channel.write(direct);
                    direct.rewind();
                    direct.limit(direct.capacity());
                    offset = 0;
                }
                int written = desc.encode(jvm, offset);
                assert written == length;
                offset += length;
                fileOffset += length;
                count++;
            }
            direct.put(jvm, 0, offset);
            direct.rewind();
            direct.limit(offset);
            channel.write(direct);
            direct.rewind();
            direct.limit(direct.capacity());
            channel.position(0);
            ByteBuffer headerData = ByteBuffer.wrap(jvm);
            Codec.getCodec().encode(docIdOffset, jvm, 0);
            byte postingsCoding = 0;
            if(InvertedList.getTableType() == TableType.VariableByteCoded) {
                postingsCoding = 1;
            }
            Codec.getCodec().encode(postingsCoding, jvm, Type.LONG.length());
            Codec.getCodec().encode(prefixLength, jvm, Type.LONG.length()+Type.BYTE.length());
            headerData.limit(Type.LONG.length()+Type.BYTE.length()+Type.INTEGER.length());
            channel.write(headerData);
            System.err.println("Dictionary items "+count);
        } catch (Throwable t) {
            log.fatal("exception writing index file " + t, t);
            throw new RuntimeException(t);
        }
        return fileOffset;
    }
    
    public int getBlockSize() {
        return BLOCK_SIZE;
    }
    
    public void setBlockSize(int blockSize) {
        BLOCK_SIZE = blockSize;
    }
}
