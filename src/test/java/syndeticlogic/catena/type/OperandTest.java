package syndeticlogic.catena.type;

import static org.junit.Assert.*;

import org.junit.Test;

import syndeticlogic.catena.type.BinaryOperand;
import syndeticlogic.catena.type.Type;

public class OperandTest {

    @Test
    public void test() {
        byte[] test = new byte[1];
        byte[] test1 = new byte[2];
        
        test[0] = 0x0;
        BinaryOperand bin = new BinaryOperand(test, 0, 1);
        assertEquals(test, bin.data());
        assertEquals(0, bin.offset());
        assertEquals(1, bin.length());
        assertEquals(Type.BINARY, bin.type());
        bin.reset(test1, 1, 1);
        
        assertEquals(test1, bin.data());
        assertEquals(1, bin.offset());
        assertEquals(1, bin.length());
        assertEquals(Type.BINARY, bin.type());
        bin.reset(test1, 1, 1);
    }

}
