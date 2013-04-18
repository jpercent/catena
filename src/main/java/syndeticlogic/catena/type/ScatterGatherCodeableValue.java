package syndeticlogic.catena.type;

import syndeticlogic.catena.utility.CodeHelper;
import syndeticlogic.catena.utility.Codec;


public class ScatterGatherCodeableValue extends ScatterGatherValue {

    public ScatterGatherCodeableValue(Codeable value) {
        super();
        CodeHelper coder = Codec.getCodec().coder();
        coder.append(value);
        byte[] rawvalue = coder.encodeByteArray();
        add(rawvalue, 0, rawvalue.length);
    }	
	
    ScatterGatherCodeableValue() {
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
