package syndeticlogic.catena.text;

import java.nio.ByteBuffer;
import java.util.Arrays;

import syndeticlogic.catena.type.Codeable;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;

public class UncompressedDocumentIdTable extends DocumentIdTable {
    private int[][] documentIds;
    private int slots;
    private int slotCursor;
    private int pageCursor;
    private int slotIterator;
    private int pageIterator;
    
    public UncompressedDocumentIdTable() {
        documentIds = new int[PAGE_SIZE][];
        documentIds[0] = new int[PAGE_SIZE];
        slots = PAGE_SIZE;        
    }
    
    public void addId(int docId) {
        if(pageCursor == PAGE_SIZE) {
            addPage();
        }   
        documentIds[slotCursor][pageCursor] = docId;
        pageCursor++;
    }

    private void addPage() {
        if(slotCursor + 1 == slots) {
            expand();
        }
        slotCursor++;
        pageCursor = 0;
        documentIds[slotCursor] = new int[PAGE_SIZE];
    }

    private void expand() {
        int[][] newDocs = new int[slots+PAGE_SIZE][];
        System.arraycopy(documentIds, 0, newDocs, 0, slots);
        slots += PAGE_SIZE;
        documentIds = newDocs;
    }
    
   
    @Override
    public int encode(byte[] dest, int offset) {
        int docIdsSize = size();
        Codec.getCodec().encode(docIdsSize, dest, offset);
        int copied = Type.INTEGER.length();
        
        for(int i = 0; i <= slotCursor; i++) {
            int length = PAGE_SIZE;
            if(i == slotCursor) {
                length = pageCursor;
            }
            ByteBuffer transferBuff = ByteBuffer.wrap(dest, offset+copied, length*Type.INTEGER.length());
            transferBuff.asIntBuffer().put(documentIds[i], 0, length);
            copied += length*Type.INTEGER.length();
        }
        return copied;
    }
        
    @Override
    public int decode(byte[] source, int offset) {
        int remaining = Codec.getCodec().decodeInteger(source, offset);
        int accumulation = Type.INTEGER.length();
        while (remaining > 0) {
            int blockSize = PAGE_SIZE;
            if(remaining/Type.INTEGER.length() < blockSize) {
                blockSize = remaining/Type.INTEGER.length();
            }
            pageCursor = blockSize;
            ByteBuffer transferBuf = ByteBuffer.wrap(source, offset+accumulation, remaining); 
            transferBuf.asIntBuffer().get(documentIds[slotCursor], 0, blockSize);
            if(pageCursor == PAGE_SIZE) {
                addPage();
            }
            remaining -= blockSize*Type.INTEGER.length();
            accumulation += blockSize*Type.INTEGER.length();
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
        return (/* filled up pages + last, partial page */ Type.INTEGER.length() * slotCursor * PAGE_SIZE) 
        + (Type.INTEGER.length() * pageCursor);
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
        UncompressedDocumentIdTable other = (UncompressedDocumentIdTable) obj;
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
        return documentIds[slotIterator][pageIterator];
    }

    public int advanceIterator() {
        int ret = documentIds[slotIterator][pageIterator++];
        if(pageIterator == PAGE_SIZE) {
            slotIterator++;
            pageIterator=0;         
        }
        return ret;
    }
         
    public int getLastDocId() {
        return documentIds[slotCursor][pageCursor-1];
    }
    
    @Override
    public String oridinal() {
        throw new RuntimeException("ordinal is unsupported");
    }
}
