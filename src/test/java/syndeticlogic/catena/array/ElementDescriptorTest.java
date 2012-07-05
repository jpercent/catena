package syndeticlogic.catena.array;


import static org.junit.Assert.*;

import org.junit.Test;

import syndeticlogic.catena.array.ValueDescriptor;
import syndeticlogic.catena.utility.CompositeKey;

public class ElementDescriptorTest {
    
    @Test
    public void test() throws Exception {
        CompositeKey c = new CompositeKey();
        c.append("Bo don't know Jack");
        ValueDescriptor et = new ValueDescriptor(c, 11, 22, 33);
        assertEquals(c, et.segmentId);
        assertEquals(11, et.segmentOffset);
        assertEquals(22, et.size);
        assertEquals(33, et.index);
        
    }
}
