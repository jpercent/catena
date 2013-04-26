package syndeticlogic.catena.text.io;

import java.util.List;
import java.util.Map;

import syndeticlogic.catena.text.DocumentDescriptor;
import syndeticlogic.catena.text.postings.IdTable.TableType;
import syndeticlogic.catena.text.postings.InvertedList;
import syndeticlogic.catena.text.postings.InvertedListDescriptor;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;

public class DictionaryReadCursor implements ReadCursor {
    private IdDocMapCursor idDocMapCursor;
    private InvertedListDescriptorCursor invertedListDescriptorCursor;
    private long idDocLength;
    private long decoded;
    
    public DictionaryReadCursor(Map<Integer, String> idToWord, List<InvertedListDescriptor> descriptors) {
        this.idDocMapCursor = new IdDocMapCursor(idToWord);
        this.invertedListDescriptorCursor = new InvertedListDescriptorCursor(descriptors);
    }
    
    public int headerSize() {
        return Type.LONG.length()+Type.BYTE.length();
    }
    
    public void decodeHeader(BlockDescriptor blockDesc) {
        byte coding = Codec.getCodec().decodeByte(blockDesc.buf, 0);
        idDocLength = Codec.getCodec().decodeLong(blockDesc.buf, 1);
        if(coding == 1) {
            InvertedList.setTableType(TableType.VariableByteCoded);
        }
        blockDesc.offset += Type.BYTE.length() + Type.LONG.length(); 
    }
    
    public void decodeBlock(BlockDescriptor blockDesc) {
        assert blockDesc.buf.length >= blockDesc.offset;
        while (blockDesc.offset < blockDesc.buf.length) {
            if (decoded < idDocLength) {
                idDocMapCursor.decodeBlock(blockDesc);
            } else {
                invertedListDescriptorCursor.decodeBlock(blockDesc);
            }
        }
    }
    
    protected class IdDocMapCursor extends BaseReadCursor {
        private DocumentDescriptor docDesc;
        private Map<Integer, String> idDocMap;
        public IdDocMapCursor(Map<Integer, String> idDocMap) {
            this.idDocMap = idDocMap;
            this.docDesc = new DocumentDescriptor();
        }
        
        @Override
        public void doDecode(BlockDescriptor blockDesc) {
            int length = docDesc.decode(blockDesc.buf, blockDesc.offset);
            idDocMap.put(docDesc.getDocId(), docDesc.getDoc());
            decoded += length;
            blockDesc.offset += length;
        }
    }
        
    protected class InvertedListDescriptorCursor extends BaseReadCursor {
        List<InvertedListDescriptor> descriptors;
        public InvertedListDescriptorCursor(List<InvertedListDescriptor> descriptors) {
            this.descriptors = descriptors;
        }
        
        @Override
        public void doDecode(BlockDescriptor blockDesc) {
            InvertedListDescriptor listDesc = new InvertedListDescriptor(null,-1,-1,-1);
            int length = listDesc.decode(blockDesc.buf, blockDesc.offset);
            descriptors.add(listDesc);
            decoded += length;
            blockDesc.offset += length;
        }
    }
}
