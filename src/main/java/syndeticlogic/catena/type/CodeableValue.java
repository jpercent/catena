package syndeticlogic.catena.type;

import syndeticlogic.catena.codec.Codec;


public class CodeableValue extends Value {
	Codeable decodedValue;
	
    CodeableValue(byte[] data, int offset, int length) {
        super(data, offset, length);
        this.decodedValue = Codec.getCodec().decodeCodeable(data, offset);
    }
    
    @Override
    public Object objectize() {
        return this.decodedValue;
    }

    @Override
    public Type type() {
        return Type.CODEABLE;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert rawBytes.length - offset >= length && length == 4;
        Codeable value = Codec.getCodec().decodeCodeable(rawBytes, offset);
        return value.compareTo(decodedValue);
    }
    
    @Override
    public void reset(byte[] data, int offset, int length) {
        super.reset(data, offset, length);
        this.decodedValue = Codec.getCodec().decodeCodeable(data, offset);
    }
}
