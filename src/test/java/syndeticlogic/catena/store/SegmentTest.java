package syndeticlogic.catena.store;

import static org.junit.Assert.*;

import org.junit.Test;


import syndeticlogic.catena.store.Segment;
import syndeticlogic.catena.store.SegmentManager;
import syndeticlogic.catena.type.Codec;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.type.TypeFactory;
import syndeticlogic.catena.utility.ArrayGenerator;
import syndeticlogic.catena.utility.CompositeKey;
import syndeticlogic.catena.utility.FixedLengthArrayGenerator;
import syndeticlogic.catena.utility.Util;
import syndeticlogic.catena.utility.VariableLengthArrayGenerator;

import java.util.List;

public class SegmentTest {

    @Test
    public void testInsertRead0() throws Exception {
        int seed = 31;
        int elementSize = 8;
        int elements = 1;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testInsertRead1() throws Exception {
        int seed = 31;
        int elementSize = 8;
        int elements = 2;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testInsertRead2() throws Exception {
        int seed = 31;
        int elementSize = 8;
        int elements = 512;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testInsertRead3() throws Exception {
        int seed = 31;
        int elementSize = 8;
        int elements = 513;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testInsertRead4() throws Exception {
        int seed = 31;
        int elementSize = 8;
        int elements = 513;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testInsertRead5() throws Exception {
        int seed = 31;
        int elementSize = 4096;
        int elements = 1;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testInsertRead6() throws Exception {
        int seed = 31;
        int elementSize = 4096;
        int elements = 2;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testInsertRead7() throws Exception {
        int seed = 31;
        int elementSize = 4096;
        int elements = 256;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testInsertRead8() throws Exception {
        int seed = 31;
        int elementSize = 8192;
        int elements = 1;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testInsertRead9() throws Exception {
        int seed = 31;
        int elementSize = 8192;
        int elements = 2;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testInsertRead10() throws Exception {
        int seed = 31;
        int elementSize = 8192;
        int elements = 128;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testInsertRead11() throws Exception {
        int seed = 31;
        int elementSize = 3;
        int elements = 1;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testInsertRead12() throws Exception {
        int seed = 31;
        int elementSize = 3;
        int elements = 1;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testInsertRead13() throws Exception {
        int seed = 31;
        int elementSize = 3;
        int elements = 2;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testInsertRead14() throws Exception {
        int seed = 31;
        int elementSize = 3;
        int elements = 1365;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testInsertRead15() throws Exception {
        int seed = 31;
        int elementSize = 3;
        int elements = 1366;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testInsertRead16() throws Exception {
        int seed = 31;
        int elementSize = 3;
        int elements = 2730;
        appendUpdateScanTest(seed, elementSize, elements, false, true, false);
    }

    @Test
    public void testVarialbleLengthInsertRead0() throws Exception {
        int seed = 31;
        int elementSize = -1;
        int elements = 1;
        appendUpdateScanTest(seed, elementSize, elements, false, false, false);
    }

    @Test
    public void testVarialbleLengthInsertRead2() throws Exception {
        int seed = 31;
        int elementSize = -1;
        int elements = 2;
        appendUpdateScanTest(seed, elementSize, elements, false, false, false);
    }

    @Test
    public void testVarialbleLengthInsertRead3() throws Exception {
        int seed = 31;
        int elementSize = -1;
        int elements = 33;
        appendUpdateScanTest(seed, elementSize, elements, false, false, false);
    }

    @Test
    public void testFixedLengthAppendUpdateScan1() throws Exception {
        int seed = 31;
        int elementSize = 8;
        int elements = 33;
        boolean flush = false;
        boolean fixedLength = true;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate);
    }

    @Test
    public void testVariableLengthAppendUpdateScan0() throws Exception {
        int seed = 31;
        int elementSize = -1;
        int elements = 3;
        boolean flush = false;
        boolean fixedLength = false;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate);
    }

    @Test
    public void testVariableLengthAppendUpdateScan1() throws Exception {
        int seed = 31;
        int elementSize = -1;
        int elements = 4;
        boolean flush = false;
        boolean fixedLength = false;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate);
    }

    @Test
    public void testVariableLengthAppendUpdateScan2() throws Exception {
        int seed = 31;
        int elementSize = -1;
        int elements = 12;
        boolean flush = false;
        boolean fixedLength = false;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate);
    }

    @Test
    public void testVariableLengthAppendUpdateScan3() throws Exception {
        int seed = 133;
        int elementSize = -1;
        int elements = 37;
        boolean flush = false;
        boolean fixedLength = false;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate);
    }

    /*
     * out of buffers test
     * 
     * @Test public void testVarialbleLengthInsertRead4() throws Exception { int
     * seed = 31; int elementSize = -1; int elements = 337; insertReadTest(seed,
     * elementSize, elements, false, false); }
     */
    @Test
    public void testInsertFlushRead0() throws Exception {
        int seed = 133;
        int elementSize = -1;
        int elements = 37;
        boolean flush = true;
        boolean fixedLength = false;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate);
    }

    @Test
    public void testInsertFlushRead0a() throws Exception {
        int seed = 61;
        int elementSize = 8;
        int elements = 1;
        boolean flush = true;
        boolean fixedLength = true;
        boolean testUpdate = false;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate);
    }

    @Test
    public void testInsertFlushRead1() throws Exception {
        int seed = 31;
        int elementSize = 8;
        int elements = 2;
        boolean flush = true;
        boolean fixedLength = true;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); //
    }

    @Test
    public void testInsertFlushRead2() throws Exception {
        int seed = 31;
        int elementSize = 8;
        int elements = 512;
        boolean flush = true;
        boolean fixedLength = true;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, true, false);
    }

    @Test
    public void testInsertFlushReadUpdate3() throws Exception {
        int seed = 71;
        int elementSize = 8;
        int elements = 513;
        boolean flush = true;
        boolean fixedLength = true;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, true, false);
    }

    @Test
    public void testInsertFlushReadUpdate4() throws Exception {
        int seed = 11;
        int elementSize = 8;
        int elements = 513;
        boolean flush = true;
        boolean fixedLength = true;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, true, false);
    }

    @Test
    public void testInsertFlushRead5() throws Exception {
        int seed = 31;
        int elementSize = 4096;
        int elements = 1;
        boolean flush = true;
        boolean fixedLength = true;
        boolean testUpdate = false;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, true, false);
    }

    @Test
    public void testInsertFlushReadUpdate6() throws Exception {
        int seed = 31;
        int elementSize = 4096;
        int elements = 2;
        boolean flush = true;
        boolean fixedLength = true;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, true, false);
    }

    @Test
    public void testInsertFlushReadUpdate7() throws Exception {
        int seed = 31;
        int elementSize = 4096;
        int elements = 256;
        boolean flush = true;
        boolean fixedLength = true;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, true, false);
    }

    @Test
    public void testInsertFlushRead8() throws Exception {
        int seed = 31;
        int elementSize = 8192;
        int elements = 1;
        boolean flush = true;
        boolean fixedLength = true;
        boolean testUpdate = false;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, true, false);
    }

    @Test
    public void testInsertFlushReadUpdate9() throws Exception {
        int seed = 31;
        int elementSize = 8192;
        int elements = 2;
        boolean flush = true;
        boolean fixedLength = true;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, true, false);
    }

    @Test
    public void testInsertFlushReadUpdate10() throws Exception {
        int seed = 31;
        int elementSize = 8192;
        int elements = 128;
        boolean flush = true;
        boolean fixedLength = true;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, true, false);
    }

    @Test
    public void testInsertFlushRead11() throws Exception {
        int seed = 31;
        int elementSize = 3;
        int elements = 1;
        boolean flush = true;
        boolean fixedLength = true;
        boolean testUpdate = false;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, true, false);
    }

    @Test
    public void testInsertFlushReadUpdate13() throws Exception {
        int seed = 31;
        int elementSize = 3;
        int elements = 2;
        boolean flush = true;
        boolean fixedLength = true;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, true, false);
    }

    @Test
    public void testInsertFlushReadUpdate14() throws Exception {
        int seed = 31;
        int elementSize = 3;
        int elements = 1365;
        boolean flush = true;
        boolean fixedLength = true;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, true, false);
    }

    @Test
    public void testInsertFlushReadUpdate15() throws Exception {
        int seed = 91;
        int elementSize = 3;
        int elements = 1366;
        boolean flush = true;
        boolean fixedLength = true;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, true, false);
    }

    @Test
    public void testInsertFlushReadUpdate16() throws Exception {
        int seed = 39;
        int elementSize = 3;
        int elements = 2730;
        boolean flush = true;
        boolean fixedLength = true;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, true, false);
    }

    @Test
    public void testVarialbleLengthInsertFlushRead0() throws Exception {
        int seed = 13;
        int elementSize = -1;
        int elements = 1;
        boolean flush = true;
        boolean fixedLength = false;
        boolean testUpdate = false;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, false, false);
    }

    @Test
    public void testVarialbleLengthInsertFlushReadUpdate2() throws Exception {
        int seed = 31;
        int elementSize = -1;
        int elements = 2;
        boolean flush = true;
        boolean fixedLength = false;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, false, false);
    }

    @Test
    public void testVarialbleLengthInsertFlushReadUpdate3() throws Exception {
        int seed = 17;
        int elementSize = -1;
        int elements = 33;
        boolean flush = true;
        boolean fixedLength = false;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate); // false, false, false);
    }

    @Test
    public void testFixedLengthAppendFlushUpdateScan1() throws Exception {
        int seed = 31;
        int elementSize = 8;
        int elements = 33;
        boolean flush = true;
        boolean fixedLength = false;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate);
    }

    @Test
    public void testVariableLengthAppendUpdateFlushScan0() throws Exception {
        int seed = 41;
        int elementSize = -1;
        int elements = 3;
        boolean flush = true;
        boolean fixedLength = false;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate);
    }

    @Test
    public void testVariableLengthAppendUpdateFlushScan1() throws Exception {
        int seed = 29;
        int elementSize = -1;
        int elements = 4;
        boolean flush = true;
        boolean fixedLength = false;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate);
    }

    @Test
    public void testVariableLengthAppendUpdateFlushScan2() throws Exception {
        int seed = 19;
        int elementSize = -1;
        int elements = 12;
        boolean flush = true;
        boolean fixedLength = false;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate);
    }

    @Test
    public void testVariableLengthAppendUpdateFlushScan3() throws Exception {
        int seed = 23;
        int elementSize = -1;
        int elements = 37;
        boolean flush = true;
        boolean fixedLength = false;
        boolean testUpdate = true;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLength,
                testUpdate);
    }

    public void appendUpdateScanTest(int seed, int elementSize, int elements,
            boolean flush, boolean fixedLengthData, boolean testUpdate)
            throws Exception {
        appendUpdateScanTest(seed, elementSize, elements, flush,
                fixedLengthData, testUpdate,
                SegmentManager.CompressionType.Snappy);
        appendUpdateScanTest(seed, elementSize, elements, flush,
                fixedLengthData, testUpdate,
                SegmentManager.CompressionType.Null);
    }

    public void appendUpdateScanTest(int seed, int elementSize, int elements,
            boolean flush, boolean fixedLengthData, boolean testUpdate,
            SegmentManager.CompressionType compressionType) throws Exception {

        Codec.configureCodec(new TypeFactory());
        int retryLimit = 2;
        PageFactory pa = new PageFactory(PageFactory.BufferPoolMemoryType.Java,
                PageFactory.CachingPolicy.PinnableLru,
                PageFactory.PageDescriptorType.Unsynchronized, retryLimit);

        ArrayGenerator fagen = null;
        if (fixedLengthData) {
            fagen = new FixedLengthArrayGenerator(seed, elementSize, elements);
        } else {
            fagen = new VariableLengthArrayGenerator(seed, elements);
        }
        SegmentManager.configureSegmentManager(compressionType,
                pa.createPageManager(null, 4096, 4096));
        SegmentManager fsa = SegmentManager.get();

        Util.delete("target/filesTest");
        CompositeKey fileId = new CompositeKey();
        fileId.append("target/filesTest");
        Segment fm = fsa.create(fileId.toString(), Type.BINARY);
        assertTrue(fm != null);

        List<byte[]> bytes = fagen.generateMemoryArray(elements);
        int i = 0;
        byte[] last = null;
        int position = 0;
        for (byte[] element : bytes) {
            fm.append(element, 0, element.length);
            // System.out.println("I "+i+"Position "+ position);
            if (testUpdate && i % 2 == 1) {
                fm.update(last, 0, position, element.length, last.length);
                position += last.length;
                last = null;
            } else {
                position += element.length;
                last = element;
            }
            assertEquals(position, (int) fm.size());
            i++;
        }

        if (flush) {
            fm.commit();
            fm = null;
            SegmentManager.configureSegmentManager(compressionType,
                    pa.createPageManager(null, 4096, 4096));
            fsa = SegmentManager.get();
            fm = fsa.lookup(fileId.toString());
        }

        assertEquals(position, (int) fm.size());
        long fileoffset = 0;
        i = 0;
        last = null;

        for (byte[] actual : bytes) {
            if (testUpdate && i % 2 == 1) {
                actual = last;
            }
            byte[] expected = new byte[actual.length];
            // System.out.println("I "+i+"Position "+ fileoffset);
            fm.scan(expected, 0, fileoffset, expected.length);
            fileoffset += actual.length;
            // System.out.println("element = " +i);
            assertArrayEquals(actual, expected);
            last = actual;
            i++;
        }
    }
}
