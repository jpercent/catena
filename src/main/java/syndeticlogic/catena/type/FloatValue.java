package syndeticlogic.catena.type;

import syndeticlogic.catena.utility.Codec;


public class FloatValue extends Value {
	private float decoded;
	
    public FloatValue(int data) {
        super(null,0,0);
        this.decoded = data;
    }
	
    public FloatValue(byte[] data, int offset) {
        super(data, offset, Type.FLOAT.length());
        assert data.length - offset >= Type.FLOAT.length();
        this.decoded = Codec.getCodec().decodeFloat(data, offset);
    }
    
    @Override
    public Object objectize() {
        return new Float(decoded);
    }

    @Override
    public Type type() {
        return Type.FLOAT;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert rawBytes.length - offset >= length && length == Type.FLOAT.length();
        float value = Codec.getCodec().decodeFloat(rawBytes, offset);
        if(decoded > value) 
            return 1;
        else if(decoded == value)
            return 0;
        else
            return -1;
    }
    
    @Override
    public void reset(byte[] data, int offset, int length) {
        assert data.length - offset >= length && length == Type.FLOAT.length();
        super.reset(data, offset, length);
        this.decoded = Codec.getCodec().decodeFloat(data, offset);
    }
}
