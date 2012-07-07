package syndeticlogic.catena.type;

import syndeticlogic.catena.codec.Codec;


public class LongValue extends Value {
	private long decoded;
	
    public LongValue(long data) {
        super(null,0,0);
        decoded = data;
    }
    
    public LongValue(byte[] data, int offset) {
        super(data, offset, Type.LONG.length());
        assert data.length - offset >= Type.LONG.length();        
        this.decoded = Codec.getCodec().decodeLong(data, offset);
    }
    
    @Override
    public Object objectize() {
        return new Long(decoded);
    }

    @Override
    public Type type() {
        return Type.LONG;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert rawBytes.length - offset >= length && length == Type.LONG.length();
        long value = Codec.getCodec().decodeLong(rawBytes, offset);
        if(decoded > value) 
            return 1;
        else if(decoded == value)
            return 0;
        else
            return -1;
    }
    
    @Override
    public void reset(byte[] data, int offset, int length) {
        assert data.length - offset >= length && length == Type.LONG.length();
        super.reset(data, offset, length);
        this.decoded = Codec.getCodec().decodeLong(data, offset);
    }
}
