package syndeticlogic.catena.type;

import syndeticlogic.catena.utility.Codec;


public class ByteValue extends Value {
	public ByteValue() {
	}
	
    public ByteValue(byte value) {
        super(null, 0, 0);
        byte[] rawvalue = new byte[1];
        Codec.getCodec().encode(value, rawvalue, 0);
        reset(rawvalue, 0, Type.BYTE.length());
    }
    
    ByteValue(byte[] data, int offset) {
        super(data, offset, Type.BYTE.length());
        assert data.length - offset >= Type.BYTE.length();
    }
    
    @Override
    public Object objectize() {
        return new Byte(Codec.getCodec().decodeByte(data, offset));
    }

    @Override
    public Type type() {
        return Type.BYTE;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert rawBytes.length - offset >= length && length == Type.BYTE.length();
        byte value = Codec.getCodec().decodeByte(rawBytes, offset);
        byte decoded = Codec.getCodec().decodeByte(data, this.offset);
        if(decoded > value)
            return 1;
        else if(decoded < value)
            return -1;
        else
            return 0;
    }
    
    @Override
    public void reset(byte[] data, int offset, int length) {
        assert data.length - offset >= Type.BYTE.length() && length == Type.BYTE.length();
        super.reset(data, offset, Type.BYTE.length());
    }
}
