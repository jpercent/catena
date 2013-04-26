package syndeticlogic.catena.text.postings;

import java.nio.ByteBuffer;
import java.util.Arrays;

import syndeticlogic.catena.type.Codeable;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;

public class UncodedIdTable extends IdTable {
    private int[][] ids;
    private int slots;
    private int slotCursor;
    private int pageCursor;
    private int slotIterator;
    private int pageIterator;
    
    public UncodedIdTable() {
        ids = new int[PAGE_SIZE][];
        ids[0] = new int[PAGE_SIZE];
        slots = PAGE_SIZE;        
    }
    
    public void addId(int id) {
        if(pageCursor == PAGE_SIZE) {
            addPage();
        }   
        ids[slotCursor][pageCursor] = id;
        pageCursor++;
    }

    private void addPage() {
        if(slotCursor + 1 == slots) {
            expand();
        }
        slotCursor++;
        pageCursor = 0;
        ids[slotCursor] = new int[PAGE_SIZE];
    }

    private void expand() {
        int[][] newIds = new int[slots+PAGE_SIZE][];
        System.arraycopy(ids, 0, newIds, 0, slots);
        slots += PAGE_SIZE;
        ids = newIds;
    }
    
   
    @Override
    public int encode(byte[] dest, int offset) {
        int idsSize = size();
        Codec.getCodec().encode(idsSize, dest, offset);
        int copied = Type.INTEGER.length();
        
        for(int i = 0; i <= slotCursor; i++) {
            int length = PAGE_SIZE;
            if(i == slotCursor) {
                length = pageCursor;
            }
            ByteBuffer transferBuff = ByteBuffer.wrap(dest, offset+copied, length*Type.INTEGER.length());
            transferBuff.asIntBuffer().put(ids[i], 0, length);
            copied += length*Type.INTEGER.length();
        }
        return copied;
    }
        
    @Override
    public int decode(byte[] source, int offset) {
        int remaining = Codec.getCodec().decodeInteger(source, offset);
        remaining -= Type.INTEGER.length();
        int accumulation = Type.INTEGER.length();
        while (remaining > 0) {
            int blockSize = PAGE_SIZE;
            if(remaining/Type.INTEGER.length() < blockSize) {
                blockSize = remaining/Type.INTEGER.length();
            }
            pageCursor = blockSize;
            ByteBuffer transferBuf = ByteBuffer.wrap(source, offset+accumulation, remaining); 
            transferBuf.asIntBuffer().get(ids[slotCursor], 0, blockSize);
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
        + (Type.INTEGER.length() * pageCursor)+Type.INTEGER.length();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(ids);
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
        UncodedIdTable other = (UncodedIdTable) obj;
        if (!Arrays.deepEquals(ids, other.ids))
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
        } else if(slotIterator == slotCursor && pageIterator < pageCursor) {
            return true;
        }
        return false;
    }
    
    public int peek() {
        return ids[slotIterator][pageIterator];
    }

    public int advanceIterator() {
        int ret = ids[slotIterator][pageIterator++];
        if(pageIterator == PAGE_SIZE) {
            slotIterator++;
            pageIterator=0;         
        }
        return ret;
    }
    
    public int getFirstId() {
        return ids[0][0];
    } 
    
    public int getLastId() {
        return ids[slotCursor][pageCursor-1];
    }
    
    @Override
    public String oridinal() {
        throw new RuntimeException("ordinal is unsupported");
    }
}
