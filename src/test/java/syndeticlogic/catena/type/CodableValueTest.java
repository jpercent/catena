package syndeticlogic.catena.type;

import static org.junit.Assert.*;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import syndeticlogic.catena.text.InvertedList;

public class CodableValueTest {
    InvertedList list;
    InvertedList list1;
    InvertedList list2;
    
    @Before
    public void setup() {   
        com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean)
                java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        long physicalMemorySize = os.getTotalPhysicalMemorySize();
        System.out.println(" memorySize = "+physicalMemorySize);
        
        long pages = (long)(physicalMemorySize * .50)/4096;
        System.out.println("pages == "+pages + " memory allocated = "+pages*4096);
        
        Random rand = new Random(1337);
        InvertedList.setPageSize(128);
        list = InvertedList.create(0);
        list.setWord("word is bond");
        list1 = InvertedList.create(0);
        list1.setWord("word is bond");
        list2 = InvertedList.create(1);
        list2.setWord("word is bondage");
        int size = InvertedList.getPageSize() * InvertedList.getPageSize() * 31;
        
        for(int i = 0; i < size; ++i) {
            if(i%3 == 0) {
                list2.addDocumentId(rand.nextInt());
            } else {
                list.addDocumentId(rand.nextInt());
                list1.addDocumentId(rand.nextInt());
            }
        }
    }
    
    @Test
    public void testCodeableValue() {
        CodeableValue value = new CodeableValue();
        CodeableValue value1 = new CodeableValue(list);
        assertTrue(0 == value1.compareTo(list));
        assertTrue(0 == value1.compareTo(list1));
        assertTrue(0 > value1.compareTo(list2));
        byte[] temp = new byte[list2.size()];
        list2.encode(temp, 0);
        boolean c = false;
        
        try {
            value.reset(temp, 0, -1);
        } catch (Throwable e) {
            c = true;
        }
        assertTrue(c);

        value.reset(list2);
        assertTrue(0 == value.compareTo(list2));
        assertTrue(0 < value.compareTo(list));
        assertTrue(0 < value.compareTo(list1));
        
        value1.reset(temp, 0, -1);
        assertTrue(0 == value.compareTo(list2));
        assertTrue(0 < value.compareTo(list));
        assertTrue(0 < value.compareTo(list1));
    }
}
