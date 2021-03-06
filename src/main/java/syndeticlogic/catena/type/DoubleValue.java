package syndeticlogic.catena.type;

import syndeticlogic.catena.utility.Codec;


public class DoubleValue extends Value {
	private double decoded;
	
	public DoubleValue() {
	}
	
    public DoubleValue(int data) {
        super(null,0,0);
        this.decoded = data;
    }
	
    public DoubleValue(byte[] data, int offset) {
        super(data, offset, Type.DOUBLE.length());
        assert data.length - offset >= Type.DOUBLE.length();
        this.decoded = Codec.getCodec().decodeDouble(data, offset);
    }
    
    @Override
    public Object objectize() {
        return new Double(decoded);
    }

    @Override
    public Type type() {
        return Type.DOUBLE;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert rawBytes.length - offset >= length && length == Type.DOUBLE.length();
        double value = Codec.getCodec().decodeDouble(rawBytes, offset);
        if(decoded > value) 
            return 1;
        else if(decoded == value)
            return 0;
        else
            return -1;
    }
    
    @Override
    public void reset(byte[] data, int offset, int length) {
        assert data.length - offset >= length && length == Type.DOUBLE.length();
        super.reset(data, offset, Type.DOUBLE.length());
        this.decoded = Codec.getCodec().decodeDouble(data, offset);
    }
}
