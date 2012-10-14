package syndeticlogic.catena.type;

public class BinaryValue extends Value {

    public BinaryValue() {
    }
    
	public BinaryValue(byte[] data, int offset, int length) {
	    super(data, offset, length);
	}
	
    @Override
    public Type type() {
        return Type.BINARY;
    }

    @Override
    public int compareTo(byte[] rawBytes, int offset, int length) {
        assert ((rawBytes.length - offset) >= length);
        boolean greater = false;
        boolean equal = true;
        boolean less = false;
        
        int theirLen = length;
        int myLen = this.length;
        
        for(int i = this.offset, j = offset, k = 0; 
            k < myLen && k < theirLen; i++, j++, k++) 
        {
            if(this.data[i] < rawBytes[j]) { 
                less = true;
                equal = false;
                break;
            } else if(this.data[i] > rawBytes[j]) {
                greater = true;
                equal = false;
                break;
            } else if(this.data[i] != rawBytes[j]) {
                equal = false;
                break;
            }
        }

        if(equal && theirLen < myLen) {
            equal = false;
            greater = true;
        } else if(equal && theirLen > myLen) {
            less = true;
            equal = false;
        }
        
        assert (greater && !(less || equal)) 
            || (less && !(greater || equal)) 
            ||(equal && !(less || greater)); 
        
        if(greater) {
            return 1;
        } else if(equal) {
            return 0;
        } else {
            assert less;
            return -1;
        }
    }
    
    @Override
    public Object objectize() {
        throw new RuntimeException("Not supported");
        //return null;
    }

}
