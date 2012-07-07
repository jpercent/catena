package syndeticlogic.catena.type;

import syndeticlogic.catena.codec.Codec;


public class BooleanValue extends Value {
	private boolean decoded;
	
    public BooleanValue(boolean data) {
        super(null, 0, 0);
        decoded = data;
    }
	
    public BooleanValue(byte[] data, int offset) {
        super(data, offset, Type.BOOLEAN.length());
        assert data.length - offset >= Type.BOOLEAN.length();
        this.decoded = Codec.getCodec().decodeBoolean(data, offset);
    }
    
    @Override
    public Object objectize() {
        return new Boolean(decoded);
    }

    @Override
    public Type type() {
        return Type.BOOLEAN;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert rawBytes.length - offset >= length && length == Type.BOOLEAN.length();
        boolean value = Codec.getCodec().decodeBoolean(rawBytes, offset);
        
        if(decoded && !value)
            return 1;
        else if(!decoded && value)
            return -1;
        else
            return 0;
    }
    
    @Override
    public void reset(byte[] data, int offset, int length) {
        assert ((data.length - offset) >= length && Type.BOOLEAN.length() == length);
        super.reset(data, offset, length);
        this.decoded = Codec.getCodec().decodeBoolean(data, offset);
    }
}
