package syndeticlogic.catena.type;

import syndeticlogic.catena.utility.Codec;


public class BooleanValue extends Value {
    public BooleanValue() {
    }

    public BooleanValue(boolean value) {
        super(null, 0, 0);
        byte[] rawvalue = new byte[Type.BOOLEAN.length()];
        Codec.getCodec().encode(value, rawvalue, 0);
        reset(rawvalue, 0, Type.BOOLEAN.length());
    }
	
    public BooleanValue(byte[] data, int offset) {
        super(data, offset, Type.BOOLEAN.length());
        assert data.length - offset >= Type.BOOLEAN.length();
    }
    
    @Override
    public Object objectize() {
        return new Boolean(Codec.getCodec().decodeBoolean(data, offset));
    }

    @Override
    public Type type() {
        return Type.BOOLEAN;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert rawBytes.length - offset >= length && length == Type.BOOLEAN.length();
        boolean value = Codec.getCodec().decodeBoolean(rawBytes, offset);
        boolean decoded = new Boolean(Codec.getCodec().decodeBoolean(data, this.offset));
        
        if(decoded && !value)
            return 1;
        else if(!decoded && value)
            return -1;
        else
            return 0;
    }
    
    @Override
    public void reset(byte[] data, int offset, int length) {
        assert (data.length - offset) >= length && Type.BOOLEAN.length() == length;
        super.reset(data, offset, Type.BOOLEAN.length());
    }
}
