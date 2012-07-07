package syndeticlogic.catena.type;

import syndeticlogic.catena.codec.Codec;


public class CodeableOperand extends Value {
	private boolean data;
	
    CodeableOperand(byte[] data, int offset, int length) {
        super(data, offset, length);
        //this.data = Codec.getCodec().decodeBoolean(data, offset);
    }
    
    @Override
    public Object objectize() {
        return new Boolean(data);
    }

    @Override
    public Type type() {
        return Type.INTEGER;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert rawBytes.length - offset >= length && length == 4;
        boolean value = Codec.getCodec().decodeBoolean(rawBytes, offset);
        
        if(data && !value)
            return 1;
        else if(!data && value)
            return -1;
        else
            return 0;
    }
    
    @Override
    public void reset(byte[] data, int offset, int length) {
        super.reset(data, offset, length);
        this.data = Codec.getCodec().decodeBoolean(data, offset);
    }
}
