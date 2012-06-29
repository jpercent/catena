package syndeticlogic.catena.type;


public class TypeOperand extends Operand {
	private Type data;
	
    
    public TypeOperand(byte[] data, int offset) {
        super(data, offset, Type.TYPE.length());
        this.data = Codec.getCodec().decodeCodecType(data, offset);
    }
    
    @Override
    public Object objectize() {
        return data;
    }

    @Override
    public Type type() {
        return Type.TYPE;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert rawBytes.length - offset >= length && length == Type.TYPE.length();
        Type value = Codec.getCodec().decodeCodecType(rawBytes, offset);
        if(data.ordinal() > value.ordinal()) 
            return 1;
        else if(data.ordinal() == value.ordinal())
            return 0;
        else 
            return -1;
    }
    
    @Override
    public void reset(byte[] data, int offset, int length) {
        super.reset(data, offset, length);
        this.data = Codec.getCodec().decodeCodecType(data, offset);
    }
}
