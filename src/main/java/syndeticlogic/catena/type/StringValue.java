package syndeticlogic.catena.type;

import syndeticlogic.catena.codec.Codec;


public class StringValue extends Value {
	private String data;
    
    public StringValue(byte[] data, int offset, int length) {
        super(data, offset, length);
        this.data = new String(data, offset, length);
    }
    
    @Override
    public Object objectize() {
        return data;
    }

    @Override
    public Type type() {
        return Type.STRING;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert rawBytes.length - offset >= length && length == 4;
        String value = Codec.getCodec().decodeString(rawBytes, offset);
        assert data != null;
        return data.compareTo(value);
    }
    
    @Override
    public void reset(byte[] data, int offset, int length) {
        super.reset(data, offset, length);
        this.data = new String(data, offset, length);
    }
}
