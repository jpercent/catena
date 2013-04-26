package syndeticlogic.catena.text.postings;

import java.nio.ByteBuffer;

import syndeticlogic.catena.type.Codeable;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;

public class UncodedIdArray extends IdTable {
    private static final int arraySize = 128;
    private int[] ids;
    private int cursor;
    private int iterator;
    
    public UncodedIdArray() {
        ids = new int[arraySize];
        cursor = 0;
        iterator = 0;
    }
    
    public void addId(int id) {
        if(cursor == ids.length) {
            expand();
        }   
        ids[cursor] = id;
        cursor++;
    }

    private void expand() {
        int[] newIds = new int[ids.length * 2];
        System.arraycopy(ids, 0, newIds, 0, ids.length);
        ids = newIds;
    }
    
    @Override
    public int encode(byte[] dest, int offset) {
        int idsSize = size();
        Codec.getCodec().encode(idsSize, dest, offset);
        int copied = Type.INTEGER.length();
        ByteBuffer transferBuff = ByteBuffer.wrap(dest, offset+copied, cursor*Type.INTEGER.length());
        transferBuff.asIntBuffer().put(ids, 0, cursor);
        copied += cursor*Type.INTEGER.length();
        return copied;
    }
        
    @Override
    public int decode(byte[] source, int offset) {
        int remaining = Codec.getCodec().decodeInteger(source, offset);
        remaining -= Type.INTEGER.length();
        int accumulation = Type.INTEGER.length();
        if(remaining > ids.length) {
            ids = new int[remaining/Type.INTEGER.length()];
        }
        ByteBuffer transferBuf = ByteBuffer.wrap(source, offset+accumulation, remaining);
        transferBuf.asIntBuffer().get(ids, 0, remaining/Type.INTEGER.length());
        cursor = remaining/Type.INTEGER.length();
        accumulation += remaining;
        return accumulation;
    }
    
    @Override
    public int compareTo(Codeable c) {
        throw new RuntimeException("Comparing document lists is unsupported at this time");
    }
    
    @Override
    public int size() {
        return Type.INTEGER.length() * cursor + Type.INTEGER.length();
    }

    public void resetIterator() {
        iterator = 0;
    }
    
    public boolean hasNext() {
        if(iterator < ids.length) {
            return true;
        } else {
            return false;
        }
    }
    
    public int peek() {
        return ids[iterator];
    }

    public int advanceIterator() {
        int ret = ids[iterator];
        iterator ++;
        return ret;
    }
         
    public int getLastId() {
        return ids[cursor-1];
    }
    
    public int getFirstId() {
        assert cursor > 0;
        return ids[0];
    }
    
    @Override
    public String oridinal() {
        throw new RuntimeException("ordinal is unsupported");
    }
}
