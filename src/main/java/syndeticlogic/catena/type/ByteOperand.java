package syndeticlogic.catena.type;

import syndeticlogic.catena.codec.Codec;


public class ByteOperand extends Operand {
	private byte decoded;
	
    ByteOperand(byte[] data, int offset) {
        super(data, offset, Type.BYTE.length());
        assert data.length - offset >= Type.BYTE.length();
        this.decoded = Codec.getCodec().decodeByte(data, offset);
    }
    
    @Override
    public Object objectize() {
        return new Byte(decoded);
    }

    @Override
    public Type type() {
        return Type.BYTE;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert rawBytes.length - offset >= length && length == Type.BYTE.length();
        byte value = Codec.getCodec().decodeByte(rawBytes, offset);
        
        if(decoded > value)
            return 1;
        else if(decoded < value)
            return -1;
        else
            return 0;
    }
    
    @Override
    public void reset(byte[] data, int offset, int length) {
        assert data.length - offset >= Type.BYTE.length();
        super.reset(data, offset, length);
        this.decoded = Codec.getCodec().decodeByte(data, offset);
    }
}
