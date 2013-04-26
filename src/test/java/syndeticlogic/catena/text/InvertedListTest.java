package syndeticlogic.catena.text;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;

import org.junit.Test;

import syndeticlogic.catena.text.postings.InvertedList;
import syndeticlogic.catena.text.postings.IdTable.TableType;

public class InvertedListTest {

    @Test
    public void testAddDocumentId() {
        byte[] array = new byte[1];
        byte[][][] table = new byte[3][][];
        table[0] = new byte[3][];
        table[0][0] = array;
        testBase(512, 512, 0, 30, false);
    }
    
    @Test
    public void testCoding() {
        InvertedList.setTableType(TableType.Uncoded);
        testBase(512, 512, 31, 33, true);
        InvertedList.setTableType(TableType.VariableByteCoded);
        testBase(51, 51, 31, 3, true);
    }
    
    @Test
    public void testCoding1() {
        InvertedList.setTableType(TableType.Uncoded);
        testBase(127, 4096, 32, 21, true);
        InvertedList.setTableType(TableType.VariableByteCoded);
        testBase(127, 4096, 32, 21, true);
    }
    
    @Test
    public void testCoding2() {
        InvertedList.setTableType(TableType.Uncoded);
        testBase(511, 4095, 768, 88, true);
        InvertedList.setTableType(TableType.VariableByteCoded);
        testBase(511, 4095, 768, 88, true);
    }
    
    @Test
    public void testCoding3() {
        InvertedList.setTableType(TableType.Uncoded);
        testBase(1024, 511, 0, 2, true);
        InvertedList.setTableType(TableType.VariableByteCoded);
        testBase(1024, 511, 0, 2, true);
    }
    
    @Test
    public void testCoding4() {
        InvertedList.setTableType(TableType.Uncoded);
        testBase(4095, 1023, 0, 2, true);
        InvertedList.setTableType(TableType.VariableByteCoded);
        testBase(4096, 511, 0, 4, true);
    }
    
    @Test
    public void testCompare() {
        InvertedList.setTableType(TableType.Uncoded);
        doCompareTest();
        InvertedList.setTableType(TableType.VariableByteCoded);
        doCompareTest();
        
    }
    public void doCompareTest() {
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
        
        TreeSet<Integer> ids = new TreeSet<Integer>();
        long start = System.currentTimeMillis();
        for(int i = 0; i < size; ++i) {
            try {
                ids.add(Math.abs(rand.nextInt()));

            } catch(Throwable t) {
              //  System.out.println("size + i "+size+ " "+i);
                throw new RuntimeException(t);
            }
        }
        
        System.out.println("Lots of time rand?"+(System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        Iterator i = ids.iterator();
        while (i.hasNext()) {
            posting.addDocumentId((Integer) i.next());
        }
        
        InvertedList posting1 = posting;
        if(compressDecompress) {
            byte[] encoded = new byte[posting.size()+offset];
            posting.encode(encoded, offset);
            InvertedList.setPageSize(postings1PageSize);        
            posting1 = InvertedList.create();
            posting1.decode(encoded, offset);
        }
        
        //rand = new Random(1337);
        posting1.resetIterator();
        i = ids.iterator();
        int count =0;
        while (i.hasNext()) {
            try {
            assertTrue(posting1.hasNext());
            int expected = (Integer) i.next();
            int actual = posting1.advanceIterator();
            //stem.out.println("I = "+count+" expected "+expected+ " actual "+actual);
            assertEquals(expected, actual);
            count++;
            } catch (Throwable t) {
                //tem.out.println(" i "+i);
                assertFalse(true);
            }
        }
        System.out.println("Lots of time ~rand?"+(System.currentTimeMillis() - start));
    }
}
