package syndeticlogic.catena.type;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.type.Type;

public class CodeableValue extends Value {
    private static final Log log = LogFactory.getLog(CodeableValue.class);
	private Codeable decoded;

	public CodeableValue() {	    
	}
	
	public CodeableValue(Codeable value) {
	    super(null, 0, 0);
	    reset(value);
	}
	
	public int compareTo(Codeable c) {
	    return decoded.compareTo(c);
	}
	
	public void reset(Codeable value) {
        length = value.size();
        data = new byte[value.size()];
        System.out.println(data+" "+length);
        length = value.encode(data, 0);
        offset = 0;
        decoded = value;	    
	}

	@Override
    public Object objectize() {
        return decoded;
    }

    @Override
    public Type type() {
        return Type.CODEABLE;
    }
	
    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        Codeable o = createCodeable(rawBytes, offset);
        return decoded.compareTo(o);
    }

    @Override
    public void reset(byte[] data, int offset, int length) {
        decoded = createCodeable(data, offset);
        decoded.decode(data, offset);
        reset(decoded);
    }

    private Codeable createCodeable(byte[] rawBytes, int offset) {
        try {
            Codeable value = decoded.getClass().newInstance();
            value.decode(data, offset);
            return value;
        } catch (Exception e) {
            log.error("Could not reset CodeableValue: " + e, e);
            throw new RuntimeException(e);
        }
    }
}
