package syndeticlogic.catena.text;

import static org.junit.Assert.*;

import java.util.Random;
import org.junit.Test;

public class InvertedListTest {

    @Test
    public void testAddDocumentId() {
        testBase(512, 512, 0, 30, false);
    }
    
    @Test
    public void testCoding() {
        testBase(512, 512, 31, 33, true);
    }
    
    @Test
    public void testCoding1() {
        testBase(1023, 4096, 32, 21, true);
    }
    
    @Test
    public void testCoding2() {
        testBase(511, 4095, 768, 88, true);
    }
    
    @Test
    public void testCoding3() {
        testBase(4096, 511, 0, 4, true);
    }
    
    @Test
    public void testCoding4() {
        testBase(4095, 1023, 0, 8, true);
    }
    
    @Test
    public void testCompare() {
        
        InvertedList posting = InvertedList.create(12);
        InvertedList posting1 = InvertedList.create(12);
        boolean e = false;
        try {
            assertEquals(-1, posting.compareTo(posting1));
        } catch(RuntimeException re) {
            e = true;
        }
        assertTrue(e);
        e = false;
        posting.setWord("za");
        try {
            assertEquals(-1, posting.compareTo(posting1));
        } catch(RuntimeException re) {
            e = true;
        }
        assertTrue(e);
        posting1.setWord("zb");
        e = false;
        try {
            assertEquals(-1, posting.compareTo(posting1));
        } catch(RuntimeException re) {
            e = true;
        }
        assertFalse(e);
        
        InvertedList posting2 = InvertedList.create(12);
        posting2.setWord("ya");
        
        InvertedList posting3 = InvertedList.create(12);
        posting3.setWord("yg");
        
        InvertedList posting4 = InvertedList.create(12);
        posting4.setWord("ygg");
        
        assertTrue(0 > posting.compareTo(posting1));
        assertEquals(0, posting1.compareTo(posting1));
        assertTrue(0 < posting1.compareTo(posting));
        assertTrue(0 < posting1.compareTo(posting2));
        assertTrue(0 > posting2.compareTo(posting3));
        assertTrue(0 < posting1.compareTo(posting3));
        assertTrue(0 < posting4.compareTo(posting3));        
    }
    
    public void testBase(int postingsPageSize, int postings1PageSize, int offset, int postingsFactor, 
            boolean compressDecompress) 
    {
        Random rand = new Random(1337);
        InvertedList.setPageSize(postingsPageSize);
        InvertedList posting = InvertedList.create(12);
        int size = InvertedList.getPageSize() * InvertedList.getPageSize() * postingsFactor;
        
        for(int i = 0; i < size; ++i) {
            posting.addDocumentId(rand.nextInt());
        }
        
        InvertedList posting1 = posting;
        if(compressDecompress) {
            byte[] encoded = new byte[posting.size()+offset];
            posting.encode(encoded, offset);
            InvertedList.setPageSize(postings1PageSize);        
            posting1 = InvertedList.create(-1);
            posting1.decode(encoded, offset);
        }
        
        rand = new Random(1337);
        posting1.resetIterator();
        for(int i = 0; i < size; ++i) {  
            assertTrue(posting1.hasNext());
            int expected = rand.nextInt();
            int actual = posting1.advanceIterator();
            //System.out.println("I = "+i+" expected "+expected+ " actual "+actual);
            assertEquals(expected, actual);
        }
    }
}
