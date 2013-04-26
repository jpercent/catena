package syndeticlogic.catena.text;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;

import org.junit.BeforeClass;
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
        System.out.println("TestCoding0 pageSize = 512, ids = "+(512*512*31));
        long start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.UncodedTable);
        testBase(512, 512, 31, 33, true);
        System.out.println("UncodedTable  time "+ (System.currentTimeMillis() - start));
        
        start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.UncodedTable);
        testBase(512, 512, 31, 33, true);
        System.out.println("UncodedArray time "+ (System.currentTimeMillis() - start));
        
        start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.VariableByteCodedTable);
        testBase(512, 512, 31, 33, true);
        System.out.println("VariableTable time "+ (System.currentTimeMillis() - start));
        
        start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.VariableByteCodedArray);
        testBase(512, 512, 31, 33, true);
        System.out.println("VariableArray time "+ (System.currentTimeMillis() - start));
    }
    
    @Test
    public void testCoding1() {
        System.out.println("TestCoding1 pageSize = 127, ids = "+(127*127*31));
        long start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.UncodedTable);
        testBase(127, 4096, 32, 21, true);
        System.out.println("UncodedTable time2 "+ (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.UncodedArray);
        testBase(127, 4096, 32, 21, true);
        System.out.println("UncodedArray time2 "+ (System.currentTimeMillis() - start));
        
        start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.VariableByteCodedTable);
        testBase(127, 4096, 32, 21, true);
        System.out.println("VariableTable time2 "+ (System.currentTimeMillis() - start));
        
        start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.VariableByteCodedArray);
        testBase(127, 4096, 32, 21, true);
        System.out.println("VariableArray time2 "+ (System.currentTimeMillis() - start));
    }
    
    @Test
    public void testCoding2() {
        System.out.println("TestCoding2 pageSize = 511, ids = "+(511*511*31));
        long start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.UncodedTable);
        testBase(511, 4095, 768, 88, true);
        System.out.println("UncodedTable time2 "+ (System.currentTimeMillis() - start));
        
        start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.UncodedArray);
        testBase(511, 4095, 768, 88, true);
        System.out.println("UncodedArray time2 "+ (System.currentTimeMillis() - start));
        
        start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.VariableByteCodedTable);
        testBase(511, 4095, 768, 88, true);
        System.out.println("VariableTable time2 "+ (System.currentTimeMillis() - start));
        
        start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.VariableByteCodedArray);
        testBase(511, 4095, 768, 88, true);
        System.out.println("VariableArray time2 "+ (System.currentTimeMillis() - start));
    }
    
    @Test
    public void testCoding3() {
        System.out.println("TestCoding3 pageSize = 1024, ids = "+(1024*1024*31));
        long start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.UncodedTable);
        testBase(1024, 511, 0, 2, true);
        System.out.println("UncodedTable time3 "+ (System.currentTimeMillis() - start));
        
        start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.UncodedArray);
        testBase(1024, 511, 0, 2, true);
        System.out.println("UncodedArry time3 "+ (System.currentTimeMillis() - start));
        
        start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.VariableByteCodedTable);
        testBase(1024, 511, 0, 2, true);
        System.out.println("VariableTable time3 "+ (System.currentTimeMillis() - start));
        
        start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.VariableByteCodedArray);
        testBase(1024, 511, 0, 2, true);
        System.out.println("VariableArray time3 "+ (System.currentTimeMillis() - start));
    }
    
    @Test
    public void testCoding4() {
        System.out.println("TestCoding4 pageSize = 4095, ids = "+(4095*4095*2));
        long start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.UncodedTable);
        testBase(4095, 1023, 0, 2, true);
        System.out.println("UncodedTable time4 "+ (System.currentTimeMillis() - start));
        
        start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.UncodedArray);
        testBase(4095, 1023, 0, 2, true);
        System.out.println("UncodedArray time4 "+ (System.currentTimeMillis() - start));
        
        start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.VariableByteCodedTable);
        testBase(4096, 511, 0, 4, true);
        System.out.println("VariableTable time4 "+ (System.currentTimeMillis() - start));
        
        start = System.currentTimeMillis();
        InvertedList.setTableType(TableType.VariableByteCodedArray);
        testBase(4096, 511, 0, 4, true);
        System.out.println("VariableArray time4 "+ (System.currentTimeMillis() - start));
    }
    
    @Test
    public void testCompare() {
        InvertedList.setTableType(TableType.UncodedTable);
        doCompareTest();
        InvertedList.setTableType(TableType.VariableByteCodedTable);
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
    static Random rand = new Random(1337);        
    static TreeSet<Integer> ids = new TreeSet<Integer>();
    static int size;
    @BeforeClass
    public static void setup() {
        System.out.println("Generating ids...");
        size = 4096*4096;
        //size = 4096;
        long start = System.currentTimeMillis();
        for(int i = 0; i < size; ++i) {
            try {
                ids.add(Math.abs(rand.nextInt()));

            } catch(Throwable t) {
              //  System.out.println("size + i "+size+ " "+i);
                throw new RuntimeException(t);
            }
        }
        System.out.println("Finish generating ids...");
    }
    public void testBase(int postingsPageSize, int postings1PageSize, int offset, int postingsFactor, 
            boolean compressDecompress) 
    {

        InvertedList.setPageSize(postingsPageSize);
        InvertedList posting = InvertedList.create(12);
        int size = InvertedList.getPageSize() * InvertedList.getPageSize() * postingsFactor;
        
  //      System.out.println("Lots of time rand?"+(System.currentTimeMillis() - start));
//        start = System.currentTimeMillis();
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
    }
}
