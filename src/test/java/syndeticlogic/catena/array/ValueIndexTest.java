package syndeticlogic.catena.array;


import static org.junit.Assert.*;

import org.junit.Test;

import syndeticlogic.catena.utility.CompositeKey;

public class ValueIndexTest {
    
    @Test
    public void valueDescriptorTest() throws Exception {
        CompositeKey c = new CompositeKey();
        c.append("Bo don't know Jack");
        ArrayDescriptor.Value vd = new ArrayDescriptor.Value(c, 11, 22222, 22, 33);
        assertEquals(c, vd.segmentId);
        assertEquals(11, vd.segmentOffset);
        assertEquals(22222, vd.byteOffset);
        assertEquals(22, vd.valueSize);
        assertEquals(33, vd.index);
    }
    
    @Test
    public void fixedLengthValueIndexTest() {
        
    }
    
    @Test
    public void variableLengthValueIndexTest() {
        
    }
}
