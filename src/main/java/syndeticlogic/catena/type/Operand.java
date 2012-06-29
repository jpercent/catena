package syndeticlogic.catena.type;

import syndeticlogic.catena.type.Type;

public abstract class Operand {
    
    protected byte[] data;
    protected int offset;
    protected int length;
    
    public Operand(byte[] data, int offset, int length) {
        reset(data, offset, length);
    }
    
    public void reset(byte[] data, int offset, int length) {
        this.data = data;
        this.offset = offset;
        this.length = length;
    }
    
    public byte[] data() {
        return data;
    }
    
    int offset() {
        return offset;
    }
    
    int length() {
        return length;
    }
    
    public abstract Object objectize();    
    public abstract Type type();
    public abstract int compareTo(byte[] rawBytes, int offset, int length);
    
}
