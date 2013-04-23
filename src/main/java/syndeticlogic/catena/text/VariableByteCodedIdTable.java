package syndeticlogic.catena.text;

import java.util.Arrays;

import syndeticlogic.catena.type.Codeable;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;
import syndeticlogic.catena.utility.VariableByteCode;

public class VariableByteCodedDocumentIdTable extends DocumentIdTable {
    private VariableByteCode codec;
    private byte[][][] documentIds;
    private int slots;
    private int slotCursor;
    private int pageCursor;
    private int slotIterator;
    private int pageIterator;
    private int lastIterator;
    
    public VariableByteCodedDocumentIdTable() {
        codec = new VariableByteCode(0);
        documentIds = new byte[PAGE_SIZE][][];
        documentIds[0] = new byte[PAGE_SIZE][];
        slots = PAGE_SIZE;        
    }
    
    public void addId(int docId) {
        int last = getLastDocId();
        assert last > docId;
        addDistance(docId - last);
    }

    private int addDistance(int distance) {
        if(pageCursor == PAGE_SIZE) {
            addPage();
        }   
        byte[] encodedDistance = codec.encode(distance);
        documentIds[slotCursor][pageCursor] = codec.encode(distance);
        pageCursor++;
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
        return copied;
    }
        
    @Override
    public int decode(byte[] source, int offset) {
        int remaining = Codec.getCodec().decodeInteger(source, offset);
        int accumulation = Type.INTEGER.length();
        while (remaining > 0) {
            int size = addDistance(codec.decode(source, offset));
            remaining -= size;
            accumulation += size;
        }
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
        VariableByteCodedDocumentIdTable other = (VariableByteCodedDocumentIdTable) obj;
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
        } else if(pageIterator < pageCursor) {
            return true;
        }
        return false;
    }
    
    public int peek() {
        byte[] id = (byte[])documentIds[slotIterator][pageIterator];
        return codec.decode(id, 0);
    }

    public int advanceIterator() {
        int ret = codec.decode((byte[])documentIds[slotIterator][pageIterator++], 0);
        ret = lastIterator + ret;
        lastIterator = ret;
        if(pageIterator == PAGE_SIZE) {
            slotIterator++;
            pageIterator=0;         
        }
        return ret;
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
        return codec.decode((byte[])documentIds[slotCursor][pageCursor-1], 0);
    }
    
    @Override
    public String oridinal() {
        throw new RuntimeException("ordinal is unsupported");
    }
}
