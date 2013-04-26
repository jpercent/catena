package syndeticlogic.catena.text.postings;

import java.util.Arrays;

import syndeticlogic.catena.type.Codeable;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;
import syndeticlogic.catena.utility.VariableByteCode;

public class VariableByteCodedIdArray extends IdTable {
    private static final int initialArraySize = 128;
    private VariableByteCode codec;
    private byte[][] ids;
    private int cursor;
    private int iterator;
    private int lastIterator;
    private int lastDocId;

    public VariableByteCodedIdArray() {
        codec = new VariableByteCode(0);
        ids = new byte[initialArraySize][];
    }
    
    public void addId(int docId) {
        if(cursor == ids.length) {
            addDistance(docId, docId);
        } else {
            int last = lastDocId;
            assert last < docId;
            addDistance(docId - last, docId);
        }
    }

    private int addDistance(int distance, int docId) {
        if(cursor == ids.length) {
            expand();
        }   
        byte[] encodedDistance = codec.encode(distance);
//      System.out.println("addDistance size = " +encodedDistance.length +" distance "+distance+" Doc id "+docId);
        ids[cursor] = encodedDistance;
        cursor++;
        lastDocId = docId;
        return encodedDistance.length;
    }

    private void expand() {
        byte[][] newDocs = new byte[ids.length*2][];
        System.arraycopy(ids, 0, newDocs, 0, ids.length);
        ids = newDocs;
    }
    
   
    @Override
    public int encode(byte[] dest, int offset) {
        int docIdsSize = size();
        Codec.getCodec().encode(docIdsSize, dest, offset);
        int copied = Type.INTEGER.length();
        resetIterator();
        while(hasNext()) {
            byte[] compressedDocId = compressedAdvanceIterator();
            System.arraycopy(compressedDocId, 0, dest, offset+copied, compressedDocId.length);
            copied += compressedDocId.length;
        }
        assert docIdsSize == copied;
        return copied;
    }
        
    @Override
    public int decode(byte[] source, int offset) {
        int remaining = Codec.getCodec().decodeInteger(source, offset);
        int totalSize = remaining;
        int accumulation = Type.INTEGER.length();
        remaining -= 4;
        boolean first = true;
        int docId;
        while (remaining > 0) {
            int distance = codec.decode(source, offset+accumulation);
            if(first) {
                docId = distance; 
                first = false;
            } else {
                 docId = lastDocId + distance;
            }
            int size = addDistance(distance, docId);
            accumulation += size;
            remaining -= size;
        }
//        System.out.println(" leaving remaining "+remaining + " accumulation "+accumulation+" total size "+totalSize);
        assert totalSize == accumulation;
        assert remaining == 0;
        return accumulation;
    }
    
    @Override
    public int compareTo(Codeable c) {
        throw new RuntimeException("Comparing document lists is unsupported at this time");
    }
    
    @Override
    public int size() {
        int size = 0;
        // XXX - no reason to compute this.  can just maintain it
        resetIterator();
        // g0d, why i has no closure?
        while(hasNext())
            size += compressedAdvanceIterator().length;
        return size + Type.INTEGER.length();
    }

    public void resetIterator() {
        iterator = 0;
        lastIterator = 0;
    }
    
    public boolean hasNext() {
        if(iterator < ids.length) {
            return true;
        } else {
            return false;
        }
    }
    
    public int peek() {
        byte[] id = ids[iterator];
        return codec.decode(id, 0);
    }

    public int advanceIterator() {
        int distance = codec.decode(ids[iterator++], 0);
        int value = lastIterator + distance; 
        lastIterator = value;
        return value;
    }
    
    private byte[] compressedAdvanceIterator() {
        byte[] ret = ids[iterator++];
        return ret;
    }

    public int getLastDocId() {
        return lastDocId;
    }
    
    @Override
    public String oridinal() {
        throw new RuntimeException("ordinal is unsupported");
    }
}
