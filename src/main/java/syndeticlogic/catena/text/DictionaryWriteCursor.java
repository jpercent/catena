package syndeticlogic.catena.text;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.text.IdTable.TableType;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;

public class DictionaryWriteCursor implements WriteCursor {
	private static final Log log = LogFactory.getLog(DictionaryWriter.class);
	private enum State { PrefixMap, IdDocMap, DescriptorList };
	private PrefixCursor prefixCursor;
	private IdDocCursor idDocCursor;
	private InvertedListDescriptorCursor invertedListCursor;
	private State state;
	
	public DictionaryWriteCursor(Map<String, Integer> prefix, Map<Integer, String> idDocMap, List<InvertedListDescriptor> descriptors) {
	    prefixCursor = new PrefixCursor(prefix);
	    idDocCursor = new IdDocCursor(idDocMap);
	    invertedListCursor = new InvertedListDescriptorCursor(descriptors);
	}
	
    @Override
    public boolean hasNext() {
        if(prefixCursor.hasNext())
            return true;
        else if (idDocCursor.hasNext())
            return true;
        else if (invertedListCursor.hasNext())
            return true;
        else
            return false;
    }

    @Override
    public int nextLength() {
        if(prefixCursor.hasNext()) {
            state = State.PrefixMap;
            return prefixCursor.nextLength();
        } else if (idDocCursor.hasNext()) {
            state = State.IdDocMap;
            return idDocCursor.nextLength();
        } else if (invertedListCursor.hasNext()) {
            state = State.DescriptorList;
            return invertedListCursor.nextLength();
        } else {
            throw new RuntimeException("Invalid state");
        }
    }

    @Override
    public int encodeNext(byte[] buffer, int offset) {        
        if(state == State.PrefixMap)
            return prefixCursor.nextLength();
        else if (state == State.IdDocMap)
            return prefixCursor.nextLength();
        else if (state == State.DescriptorList)
            return prefixCursor.nextLength();
        else
            throw new RuntimeException("Invalid state");
    }

    public class PrefixCursor extends BaseWriteCursor {
        private Iterator<Map.Entry<String, Integer>> iterator;
        private Map.Entry<String, Integer> current;
        
        public PrefixCursor(Map<String, Integer> prefix) {
            iterator = prefix.entrySet().iterator();
            headerSize = Type.LONG.length();
        }

        @Override
        public void setFileOffset(long fileOffset) {
            this.fileOffset = fileOffset;
        }   
        
        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public int nextLength() {
            current = iterator.next();
            return current.getKey().length()+Type.STRING.length()+Type.INTEGER.length();
        }

        @Override
        public int encodeNext(byte[] buffer, int offset) {
            int written = Codec.getCodec().encode(current.getValue(), buffer, offset);
            written += Codec.getCodec().encode(current.getKey(), buffer, offset+Type.INTEGER.length());
            bytesWritten += written;
            return written;
        }

        @Override
        public int encodeHeader(byte[] buffer, int offset) {
            return Codec.getCodec().encode(fileOffset + bytesWritten, buffer, offset);
        } 
    }

    public class IdDocCursor extends BaseWriteCursor {
        private Iterator<Map.Entry<Integer, String>> iterator;
        private DocumentDescriptor current;
        
        public IdDocCursor(Map<Integer, String> idToDoc) {
            this.iterator = idToDoc.entrySet().iterator();
            this.headerSize = Type.LONG.length();
        }
      
        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public int nextLength() {
            Map.Entry<Integer, String> next = iterator.next();
            current.setDocId(next.getKey());
            File f = new File(next.getValue());
            current.setDoc(f.getParentFile().getName()+File.separator+f.getName());
            return current.size();
        }

        @Override
        public int encodeNext(byte[] buffer, int offset) {
            int written = current.encode(buffer, offset);
            bytesWritten += written;
            return written;
        }

        @Override
        public int encodeHeader(byte[] buffer, int offset) {
            return Codec.getCodec().encode(fileOffset + bytesWritten, buffer, offset);
        }
    }
    
    public class InvertedListDescriptorCursor extends BaseWriteCursor {
        private Iterator<InvertedListDescriptor> iterator;
        private InvertedListDescriptor current;
        
        public InvertedListDescriptorCursor(List<InvertedListDescriptor> list) {
            iterator = list.iterator();
        }
        
        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public int nextLength() {
            current = iterator.next();
            return current.size();
        }

        @Override
        public int encodeNext(byte[] buffer, int offset) {
            return current.encode(buffer, offset);
        }    
    }

    @Override
    public int reserveHeaderLength() {
        return prefixCursor.reserveHeaderLength() + idDocCursor.reserveHeaderLength() + invertedListCursor.reserveHeaderLength() + Type.BYTE.length();
    }

    @Override
    public void setFileOffset(long fileOffset) {
        prefixCursor.setFileOffset(fileOffset);
    }

    @Override
    public int encodeHeader(byte[] buffer, int offset) {
        byte postingsCodec = 0;
        int start = offset;
        if(InvertedList.getTableType() == TableType.VariableByteCoded) {
            postingsCodec = 1;
        }
        offset += Codec.getCodec().encode(postingsCodec, buffer, offset);
        offset += prefixCursor.encodeHeader(buffer, offset);
        offset += idDocCursor.encodeHeader(buffer, offset);
        offset += invertedListCursor.encodeHeader(buffer, offset);
        return offset - start;
    }
}
