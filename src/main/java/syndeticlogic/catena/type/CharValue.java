package syndeticlogic.catena.type;

import syndeticlogic.catena.codec.Codec;


public class CharValue extends Value {
	
	public CharValue(char value) {
	    super(null, 0, 0);
	    byte[] rawvalue = new byte[Type.CHAR.length()];
	    Codec.getCodec().encode(value, rawvalue, 0);
	    reset(rawvalue, 0, Type.CHAR.length());
	}
	
    public CharValue(byte[] data, int offset) {
        super(data, offset, Type.CHAR.length());
        assert data.length - offset >= Type.CHAR.length();
    }
    
    @Override
    public Object objectize() {
        return new Character(Codec.getCodec().decodeChar(data, offset));
    }

    @Override
    public Type type() {
        return Type.CHAR;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert rawBytes.length - offset >= length && length == Type.CHAR.length();
        char value = Codec.getCodec().decodeChar(rawBytes, offset);
        char decoded = Codec.getCodec().decodeChar(data, this.offset);
        if(decoded > value) 
            return 1;
        else if(decoded == value)
            return 0;
        else
            return -1;
    }
    
    @Override
    public void reset(byte[] data, int offset, int length) {
        assert data.length - offset >= length && length == Type.CHAR.length();
        super.reset(data, offset, length);
    }
}
