package syndeticlogic.catena.type;

import syndeticlogic.catena.codec.Codec;


public class CharValue extends Value {
	private char decoded;
	
	public CharValue(char data) {
	    super(null, 0, 0);
	    decoded = data;
	}
	
    public CharValue(byte[] data, int offset) {
        super(data, offset, Type.CHAR.length());
        assert data.length - offset >= Type.CHAR.length();
        this.decoded = Codec.getCodec().decodeChar(data, offset);
    }
    
    @Override
    public Object objectize() {
        return new Character(decoded);
    }

    @Override
    public Type type() {
        return Type.CHAR;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert rawBytes.length - offset >= length && length == Type.CHAR.length();
        char value = Codec.getCodec().decodeChar(rawBytes, offset);
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
        this.decoded = Codec.getCodec().decodeChar(data, offset);
    }
}
