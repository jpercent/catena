package syndeticlogic.catena.type;

import static org.junit.Assert.*;

import org.junit.Test;

import syndeticlogic.catena.type.BinaryValue;
import syndeticlogic.catena.type.Type;

public class OperandTest {

    @Test
    public void test() {
        byte[] test = new byte[1];
        byte[] test1 = new byte[2];
        
        test[0] = 0x0;
        BinaryValue bin = new BinaryValue(test, 0, 1);
        assertArrayEquals(test, bin.data());
        assertEquals(0, bin.offset());
        assertEquals(1, bin.length());
        assertEquals(Type.BINARY, bin.type());
        
        bin.reset(test1, 1, 1);
        assertEquals(test1[1], bin.data()[0]);
        assertEquals(1, bin.data().length);
        assertEquals(0, bin.offset());
        assertEquals(1, bin.length());
        assertEquals(Type.BINARY, bin.type());
    }
}
