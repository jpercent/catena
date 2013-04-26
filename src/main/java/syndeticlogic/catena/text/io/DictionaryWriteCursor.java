package syndeticlogic.catena.text.io;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.text.DocumentDescriptor;
import syndeticlogic.catena.text.io.old.DictionaryWriter;
import syndeticlogic.catena.text.postings.InvertedList;
import syndeticlogic.catena.text.postings.InvertedListDescriptor;
import syndeticlogic.catena.text.postings.IdTable.TableType;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;

public class DictionaryWriteCursor implements WriteCursor {
	private static final Log log = LogFactory.getLog(DictionaryWriter.class);
	private enum State { IdDocMap, DescriptorList };
	private IdDocCursor idDocCursor;
	private InvertedListDescriptorCursor invertedListCursor;
	private State state;
	private long fileOffset;
	
	public DictionaryWriteCursor(Map<Integer, String> idDocMap, List<InvertedListDescriptor> descriptors) {
	    idDocCursor = new IdDocCursor(idDocMap);
	    invertedListCursor = new InvertedListDescriptorCursor(descriptors);
	}
	
    @Override
    public boolean hasNext() {
        if (idDocCursor.hasNext())
            return true;
        else if (invertedListCursor.hasNext())
            return true;
        else
            return false;
    }

    @Override
    public int nextLength() {
        if (idDocCursor.hasNext()) {
            state = State.IdDocMap;
            return idDocCursor.nextLength();
        } else if (invertedListCursor.hasNext()) {
            if(state != State.IdDocMap) {
                idDocCursor.setFileOffset(fileOffset);
            }
            state = State.DescriptorList;
            return invertedListCursor.nextLength();
        } else {
            log.fatal("Invalid state");
            throw new RuntimeException("Invalid state");
        }
    }

    @Override
    public int encodeNext(byte[] buffer, int offset) {        
        if (state == State.IdDocMap)
            return idDocCursor.encodeNext(buffer, offset);
        else if (state == State.DescriptorList)
            return invertedListCursor.encodeNext(buffer, offset);
        else
            throw new RuntimeException("Invalid state");
    }

    public class IdDocCursor extends BaseWriteCursor {
        private Iterator<Map.Entry<Integer, String>> iterator;
        private DocumentDescriptor current;
        
        public IdDocCursor(Map<Integer, String> idToDoc) {
            this.iterator = idToDoc.entrySet().iterator();
            this.headerSize = Type.LONG.length();
            this.current = new DocumentDescriptor();
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
            //System.out.println("Doc size "+current.size());
            return current.size();
        }

        @Override
        public int encodeNext(byte[] buffer, int offset) {
            int written = current.encode(buffer, offset);
            bytesWritten += written;
            //System.out.print("doc:docId "+current.getDocId()+":"+current.getDoc()+" offset "+offset+" ");BlockReader.printBinary(buffer, offset, written);
            return written;
        }

        @Override
        public int encodeHeader(byte[] buffer, int offset) {
            //    System.out.print("encodeHeader Offset = "+offset+ " bytre written "+(fileOffset+bytesWritten));
            int ret = Codec.getCodec().encode(fileOffset + bytesWritten, buffer, offset);
      //              //    System.out.print("encodeHeader Offset = "+offset+ " bytre written "+(fileOffset+bytesWritten));BlockReader.printBinary(buffer, offset, ret);
            return ret;
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
            int ret = current.encode(buffer, offset);
            //System.out.print("word:ldeng "+current.getWord()+":"+current.getLength()+" offset "+ offset+ " ");BlockReader.printBinary(buffer, offset, ret);
            return ret;
        }    
    }

    @Override
    public int reserveHeaderLength() {
        return Type.BYTE.length()+idDocCursor.reserveHeaderLength()+invertedListCursor.reserveHeaderLength();
    }

    @Override
    public int encodeHeader(byte[] buffer, int offset) {
        byte postingsCodec = 0;
        int start = offset;
        if(InvertedList.getTableType() == TableType.VariableByteCodedTable) {
            postingsCodec = 1;
        }
        offset += Codec.getCodec().encode(postingsCodec, buffer, offset);
        offset += idDocCursor.encodeHeader(buffer, offset);
        offset += invertedListCursor.encodeHeader(buffer, offset);
        return offset - start;
    }

    @Override
    public void setFileOffset(long fileOffset) {
        idDocCursor.setFileOffset(reserveHeaderLength()+fileOffset);
    }
}
