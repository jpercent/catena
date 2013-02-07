package syndeticlogic.catena.type;

import syndeticlogic.catena.utility.Codec;

public class StringValue extends ScatterGatherValue {
	public StringValue() {
	    super();
	}
	
    public StringValue(byte[] data, int offset, int length) {
        super();
        add(data, offset, length);
    }
    
    @Override
    public Object objectize() {
        return new String(gather(), offset(), length());
    }

    @Override
    public Type type() {
        return Type.STRING;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert rawBytes.length - offset >= length && length == 4;
        String value = Codec.getCodec().decodeString(rawBytes, offset);
        return value.compareTo(new String(gather(), offset(), length()));
    }
    
    @Override
    public void reset(byte[] data, int offset, int length) {
        super.reset(data, offset, length);
    }
}
