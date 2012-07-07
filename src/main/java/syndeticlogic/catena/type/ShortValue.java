package syndeticlogic.catena.type;

import syndeticlogic.catena.codec.Codec;


public class ShortValue extends Value {
	private short decoded;
	
	public ShortValue(short data) {
	    super(null,0,0);
	    decoded = data;
	}
	
    public ShortValue(byte[] data, int offset) {
        super(data, offset, Type.SHORT.length());
        assert data.length - offset >= length();
        this.decoded = Codec.getCodec().decodeShort(data, offset);
    }
    
    @Override
    public Object objectize() {
        return new Short(decoded);
    }

    @Override
    public Type type() {
        return Type.SHORT;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert rawBytes.length - offset >= length && length == 4;
        short value = Codec.getCodec().decodeShort(rawBytes, offset);
        if(decoded > value) 
            return 1;
        else if(decoded == value)
            return 0;
        else
            return -1;
    }
    
    @Override
    public void reset(byte[] data, int offset, int length) {
        assert data.length - offset >= length && length == Type.SHORT.length();
        super.reset(data, offset, length);
        this.decoded = Codec.getCodec().decodeShort(data, offset);
    }
}
