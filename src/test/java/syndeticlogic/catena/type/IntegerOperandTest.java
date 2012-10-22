package syndeticlogic.catena.type;

import static org.junit.Assert.*;

import org.junit.Test;

import syndeticlogic.catena.type.IntegerValue;
import syndeticlogic.catena.utility.Codec;

public class IntegerOperandTest {

    @Test
    public void test() {
        byte[] test = new byte[33];
        byte[] test1 = new byte[21];
        Codec.configureCodec(null);
        Codec.getCodec().encode(31, test, 17);
        Codec.getCodec().encode(31, test1, 12);    
      
        IntegerValue inte = new IntegerValue(test, 17);
        int ret = inte.compareTo(test1, 12, 4);
        assertEquals(0, ret);
        
        Codec.getCodec().encode(33, test1, 12);
        ret = inte.compareTo(test1, 12, 4);
        assertEquals(-1, ret);
        
        Codec.getCodec().encode(30, test1, 12);
        ret = inte.compareTo(test1, 12, 4);
        assertEquals(1, ret);
        
        
        Codec.getCodec().encode(13, test1, 0);
        inte.reset(test1, 0, 4);
        
        ret = inte.compareTo(test, 17, 4);
        assertEquals(-1, ret);
    }

}
