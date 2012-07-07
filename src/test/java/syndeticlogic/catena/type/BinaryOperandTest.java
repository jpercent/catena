package syndeticlogic.catena.type;

import static org.junit.Assert.*;

import org.junit.Test;

import syndeticlogic.catena.type.BinaryValue;

public class BinaryOperandTest {

    @Test
    public void test() {
        byte[] test = new byte[1];
        byte[] test1 = new byte[2];
        //byte[] test2 = new byte[3];
        //byte[] test3 = new byte[3];
        
        test[0] = 0x0;
        BinaryValue bin = new BinaryValue(test, 0, 1);
                
        test1[0] = 0x0;
        int ret = bin.compareTo(test1, 0, 1);
        assertEquals(0, ret);
        ret = bin.compareTo(test1, 0, 2);
        assertEquals(-1, ret);
        test1[0] = -1;
        ret = bin.compareTo(test1, 0, 2);
        assertEquals(1, ret);
        
        test1[1] = 0;
        ret = bin.compareTo(test1, 1, 1);
        assertEquals(0, ret);
    }

}
