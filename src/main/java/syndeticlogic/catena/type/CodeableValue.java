package syndeticlogic.catena.type;

import syndeticlogic.catena.utility.CodeHelper;
import syndeticlogic.catena.utility.Codec;


public class CodeableValue extends ScatterGatherValue {

    public CodeableValue(Codeable value) {
        super();
        CodeHelper coder = Codec.getCodec().coder();
        coder.append(value);
        byte[] rawvalue = coder.encodeByteArray();
        add(rawvalue, 0, rawvalue.length);
    }	
	
    CodeableValue() {
        super();
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
        Codeable decodedValue = Codec.getCodec().decodeCodeable(gather(), offset());
        return value.compareTo(decodedValue);
    }
}
