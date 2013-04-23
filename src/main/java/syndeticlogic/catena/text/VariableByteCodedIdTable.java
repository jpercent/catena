package syndeticlogic.catena.text;

import java.util.Arrays;

import syndeticlogic.catena.type.Codeable;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;
import syndeticlogic.catena.utility.VariableByteCode;

public class VariableByteCodedIdTable extends IdTable {
    private VariableByteCode codec;
    private byte[][][] documentIds;
    private int slots;
    private int slotCursor;
    private int pageCursor;
    private int slotIterator;
    private int pageIterator;
    private int lastIterator;
    private int lastDocId;

    public VariableByteCodedIdTable() {
        codec = new VariableByteCode(0);
        documentIds = new byte[PAGE_SIZE][][];
        documentIds[0] = new byte[PAGE_SIZE][];
        slots = PAGE_SIZE;        
    }
    
    public void addId(int docId) {
        if(slotCursor == 0 && pageCursor == 0) {
            addDistance(docId, docId);
        } else {
            int last = lastDocId;
            assert last < docId;
            addDistance(docId - last, docId);
        }
    }

    private int addDistance(int distance, int docId) {
        if(pageCursor == PAGE_SIZE) {
            addPage();
        }   
        byte[] encodedDistance = codec.encode(distance);
//      System.out.println("addDistance size = " +encodedDistance.length +" distance "+distance+" Doc id "+docId);
        documentIds[slotCursor][pageCursor] = encodedDistance;
        pageCursor++;
        lastDocId = docId;
        return encodedDistance.length;
    }
    
    private void addPage() {
        if(slotCursor + 1 == slots) {
            expand();
        }
        slotCursor++;
        pageCursor = 0;
        documentIds[slotCursor] = new byte[PAGE_SIZE][];
    }

    private void expand() {
        byte[][][] newDocs = new byte[slots+PAGE_SIZE][][];
        System.arraycopy(documentIds, 0, newDocs, 0, slots);
        slots += PAGE_SIZE;
        documentIds = newDocs;
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
//        System.out.println("posting siz e "+copied);
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(documentIds);
        result = prime * result + pageCursor;
        result = prime * result + pageIterator;
        result = prime * result + slotCursor;
        result = prime * result + slotIterator;
        result = prime * result + slots;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        VariableByteCodedIdTable other = (VariableByteCodedIdTable) obj;
        if (!Arrays.deepEquals(documentIds, other.documentIds))
            return false;
        if (pageCursor != other.pageCursor)
            return false;
        if (pageIterator != other.pageIterator)
            return false;
        if (slotCursor != other.slotCursor)
            return false;
        if (slotIterator != other.slotIterator)
            return false;
        if (slots != other.slots)
            return false;
        return true;
    }

    public void resetIterator() {
        slotIterator = 0;
        pageIterator = 0;
        lastIterator = 0;
    }
    
    public boolean hasNext() {
        if(slotIterator < slotCursor) {
            return true;
        } else if(slotIterator == slotCursor && pageIterator < pageCursor) {
            return true;
        }
        return false;
    }
    
    public int peek() {
        byte[] id = documentIds[slotIterator][pageIterator];
        return codec.decode(id, 0);
    }

    public int advanceIterator() {
        int distance = codec.decode(documentIds[slotIterator][pageIterator++], 0);
        int value = lastIterator + distance; 
        lastIterator = value;
        if(pageIterator == PAGE_SIZE) {
            slotIterator++;
            pageIterator=0;         
        }
        return value;
    }
    
    private byte[] compressedAdvanceIterator() {
        byte[] ret = documentIds[slotIterator][pageIterator++];
        if(pageIterator == PAGE_SIZE) {
            slotIterator++;
            pageIterator=0;         
        }
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
