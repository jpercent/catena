package syndeticlogic.catena.text;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.text.IdTable.TableType;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;

public class DictionaryWriter {
	private static final Log log = LogFactory.getLog(DictionaryWriter.class);
	private static int BLOCK_SIZE = 1048576/2/2;	
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
        try {
            int prefixLength = 0;
            int header = Type.LONG.length()+Type.BYTE.length()+Type.INTEGER.length();
            channel.position(header);
            fileOffset = header;
                    
            byte[] jvm = new byte[BLOCK_SIZE];
            int offset = 0;
            
            for(Map.Entry<String, Integer> document : prefixes.entrySet()) {
                int length = document.getKey().length()+Type.STRING.length()+Type.INTEGER.length();
                if (offset + length >= BLOCK_SIZE) {
                    direct.put(jvm, 0, offset);
                    direct.rewind();
                    direct.limit(offset);
                    channel.write(direct);
                    direct.rewind();
                    direct.limit(direct.capacity());
                    offset = 0;
                }
                int written = Codec.getCodec().encode(document.getValue(), jvm, offset);
                written += Codec.getCodec().encode(document.getKey(), jvm, offset+Type.INTEGER.length());
                assert written == length;
                offset += length;
                fileOffset += length;
            }
            
            prefixLength = (int)fileOffset;
            direct.put(jvm, 0, offset);
            direct.rewind();
            direct.limit(offset);
            channel.write(direct);
            direct.rewind();
            direct.limit(direct.capacity());
            
            offset = 0;
            for (Map.Entry<Integer, String> document : idToDoc.entrySet()) {
                doc.setDocId(document.getKey());
                doc.setDoc(new File(document.getValue()).getName());
                doc.setDocPrefixId(prefixes.get(new File(document.getValue()).getParent()));
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
