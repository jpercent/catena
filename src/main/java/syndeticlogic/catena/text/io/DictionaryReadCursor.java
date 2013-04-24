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
    
    public int decodeHeader(byte[] block, int blockSize) {
        byte coding = Codec.getCodec().decodeByte(block, Type.LONG.length());
        idDocLength = Codec.getCodec().decodeLong(block, 0);
        if(coding == 1) {
            InvertedList.setTableType(TableType.VariableByteCoded);
        }
        return 0;
    }
    
    public int decodeBlock(byte[] block, int blockOffset) {
        int remaining = blockOffset;
        int start = blockOffset;
        while (remaining < blockOffset) {
            int decodeLength=0;
            if (decoded < idDocLength) {
                decodeLength = idDocMapCursor.decodeBlock(block, blockOffset);
            } else {
                decodeLength = invertedListDescriptorCursor.decodeBlock(block, blockOffset);
            }
            remaining -= decodeLength;
            decoded += decodeLength;
            blockOffset += decodeLength;
        }
        return blockOffset - start; 
    }
    
    protected class IdDocMapCursor extends BaseReadCursor {
        private DocumentDescriptor docDesc;
        private Map<Integer, String> idDocMap;
        public IdDocMapCursor(Map<Integer, String> idDocMap) {
            this.idDocMap = idDocMap;
            this.docDesc = new DocumentDescriptor();
        }
        
        @Override
        public int decodeBlock(byte[] block, int blockOffset) {
            try {
            blockOffset += docDesc.decode(block, blockOffset);
            idDocMap.put(docDesc.getDocId(), docDesc.getDoc());
            } catch(IndexOutOfBoundsException e){
                addLeftover(block, blockOffset);
                blockOffset = block.length;
            }
            return blockOffset;
        }
    }
        
    protected class InvertedListDescriptorCursor extends BaseReadCursor {
        List<InvertedListDescriptor> descriptors;
        
        public InvertedListDescriptorCursor(List<InvertedListDescriptor> descriptors) {
            this.descriptors = descriptors;
        }
        
        @Override
        public int decodeBlock(byte[] block, int blockOffset) {
            InvertedListDescriptor listDesc = new InvertedListDescriptor(null, -1, -1, -1);
              try {
                  int length = listDesc.decode(block, blockOffset);
                  descriptors.add(listDesc);
                  blockOffset += length;
                  decoded += length;
              
            } catch (IndexOutOfBoundsException e) {
                addLeftover(block, blockOffset);
                blockOffset = block.length;
            }
              return blockOffset;
        }
        
    }

}
