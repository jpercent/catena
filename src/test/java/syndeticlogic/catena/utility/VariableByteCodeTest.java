package syndeticlogic.catena.utility;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Test;

public class VariableByteCodeTest {

    @Test
    public void test() {
        int input;
        VariableByteCode code = new VariableByteCode(0);
        Random rand = new Random(1337);
        for(int i =0 ; i < 1024*1024*128; i++) {
            input = Math.abs(rand.nextInt());
            code.setValue(input);
            assertEquals(input, code.value());
        }
        code.setValue(0);
        assertEquals(0, code.value());
    }
}
