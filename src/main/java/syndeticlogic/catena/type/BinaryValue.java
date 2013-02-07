package syndeticlogic.catena.type;

public class BinaryValue extends ScatterGatherValue {

    public BinaryValue() {
        super();
    }
    
	public BinaryValue(byte[] data, int offset, int length) {
	    super();
	    add(data, offset, length);
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
        int rawBytesIter = offset;
        int myLen = 0;
        int theirLen = length;
        int cumLen = 0;

        for (PartialValue value : values) {
            int segmentLen = value.length;
            int count = 0;
            for (int dataIter = value.offset; count < segmentLen && cumLen < theirLen; dataIter++, rawBytesIter++, count++, cumLen++) {
                if (value.data[dataIter] < rawBytes[rawBytesIter]) {
                    less = true;
                    equal = false;
                    break;
                } else if (value.data[dataIter] > rawBytes[rawBytesIter]) {
                    greater = true;
                    equal = false;
                    break;
                } else if (value.data[dataIter] != rawBytes[rawBytesIter]) {
                    equal = false;
                    break;
                }
            }       
            
            if(!equal) { 
                assert greater || less;
                break;
            }
            myLen += count;
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
