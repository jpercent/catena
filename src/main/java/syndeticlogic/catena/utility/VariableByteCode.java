package syndeticlogic.catena.utility;

import java.util.Arrays;

import syndeticlogic.catena.type.Codeable;

public class VariableByteCode implements Codeable {
    private byte[] encodedValue;
    
    public VariableByteCode(int value) {
        encodedValue = encode(value);
    }

    @Override
    public int encode(byte[] dest, int offset) {
        System.arraycopy(encodedValue, 0, dest, offset, encodedValue.length);
        return 0;
    }
    
    public void setValue(int value) {
        encodedValue = encode(value);
    }
    
    private byte[] encode(int value) {
        byte[] ret = null;
        long newValue = 0;
        long carry = 0;
        int current = 0;
        // i don't always write comments, but when i do...
        long byteOffset = 0;
        int size = 1; // number of bytes to be written
        long continuanceBit = 0x80; // indicates the next byte has more data
        int carryBits = 0x80; // indicates how many bits are being carried over (1, 2, 3 or 4)
        int carryShift = 0; // these are the actual accumulated bits.  for carry over on the 4th byte there are 3 shift bits.

        while (size < 5) {
            current = ((value >>> byteOffset) & 0xff); // next byte
            newValue |= (((long)(current << carryShift) & 0xff) << byteOffset); // mix it in
            if((current & carryBits) != 0 || (long)value >= (long)(carryBits << byteOffset)) { // carry required
                carry = ((current & carryBits) >>> (7 - carryShift)); // get the carry over bits
                newValue |= ((long)carry) << (byteOffset + 8);  // copy the carry  into the next byte of newValue
                newValue |= continuanceBit; // set continuance bit
                if(size == 4) {
                    ret = encode(newValue, ++size); // add another byte for the final carry
                    return ret;
                } 
            } else {
                ret = encode(newValue, size); // ship it
                return ret;
            }
            size++;
            byteOffset += 8;
            continuanceBit <<= 8;
            carryShift++;
            carryBits |= 1 << (7 - carryShift);
        }
        assert false;
        return null;
    }
    
    private byte[] encode(long value, int bytes) {
        byte[] ret = new byte[bytes];
        for(int i=0; i < bytes; i++) {
            ret[i] = (byte)((value >>> (i*8)) & 0xff);            
        }
        return ret;
    }
    
    @Override
    public int decode(byte[] code, int offset) {
        int ret = 0;
        int carryBit = 0x80;
        int carryShift = 0x7;
        int carryMask = 0x1;
        int iteration = 0;
        while (iteration < 4) {
            if((code[offset] & carryBit) != 0) {
                code[offset] = (byte)(code[offset] & ((~(1 << 7)) & 0xff)); // clear the carry bit
                ret |= (code[offset] >>> iteration) << iteration*8; // set the data for this byte
                ret |= ((code[offset+1] & carryMask) << carryShift); // bring over the carried bit
            } else {
                ret |= (code[offset] >>> iteration) << iteration*8; // set the data for this byte
                return ret; // ship it
            }
            carryShift += 7; // moves by 7s
            iteration++;
            offset++;
            carryMask |= (1 << iteration); // number of bits carried; mask values are 1, 3, 7 and f
        }
        return ret;
    }
    
    public int value() {
        return decode(encodedValue, 0);
    }
    
    @Override
    public int size() {
        return encodedValue.length;
    }

    @Override
    public int compareTo(Codeable c) {
        assert c instanceof VariableByteCode;
        VariableByteCode code = (VariableByteCode)c;
        int a = value();
        int b = code.value();
        if(a < b) {
            return -1;
        } else if(a == b) {
            return 0;
        } else {
            return 1;
        }
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(encodedValue);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        VariableByteCode other = (VariableByteCode) obj;
        if (!Arrays.equals(encodedValue, other.encodedValue))
            return false;
        return true;
    }

    @Override
    public String oridinal() {
        throw new RuntimeException("Unsupported");
    }
}