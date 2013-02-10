package syndeticlogic.catena.type;

import syndeticlogic.catena.utility.CodeHelper;
import syndeticlogic.catena.utility.Codec;


public class CodeableValue extends Value {

    public CodeableValue(Codeable value) {
        super(null, 0, 0);
        CodeHelper coder = Codec.getCodec().coder();
        coder.append(value);
        byte[] rawvalue = coder.encodeByteArray();
        reset(rawvalue, 0, rawvalue.length);
    }	
	
    CodeableValue(byte[] data, int offset, int length) {
        super(data, offset, length);
    }
    
    @Override
    public Object objectize() {
        throw new RuntimeException("Unsupprted");
    }

    @Override
    public Type type() {
        return Type.CODEABLE;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert rawBytes.length - offset >= length && length == 4;
        Codeable value = Codec.getCodec().decodeCodeable(rawBytes, offset);
        Codeable decodedValue = Codec.getCodec().decodeCodeable(data, this.offset);
        return value.compareTo(decodedValue);
    }
    
    @Override
    public void reset(byte[] data, int offset, int length) {
        super.reset(data, offset, length);
    }
}
