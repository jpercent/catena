package syndeticlogic.catena.store;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import syndeticlogic.catena.predicate.AtomicPredicate;
import syndeticlogic.catena.predicate.BinaryConnector;
import syndeticlogic.catena.predicate.CompoundPredicate;
import syndeticlogic.catena.predicate.Operator;
import syndeticlogic.catena.store.Page;
import syndeticlogic.catena.store.PageFactory;
import syndeticlogic.catena.store.PageManager;
import syndeticlogic.catena.type.IntegerValue;
import syndeticlogic.catena.utility.Codec;

public class PageIOTest {
    String sep = System.getProperty("file.separator");
    String name = "target" + sep + "pageIOHandlerTest" + sep;
    PageFactory pf;
    PageManager pm;
    Random rand;
    int pages;
    int pageSize;
    PageScan pioS;
    PageUpdate pioU;
    PageAppend pioA;
    
    @After
    public void tearDown() throws Exception {
        pm = null;
        pf = null;
    }

    @Before
    public void setup() throws Exception {
        pages = 4096;
        pageSize = 4096;
        setupHelper();
    }

    public void setupHelper() {
        rand = new Random();
        int retryLimit = 2;
        pf = new PageFactory(PageFactory.BufferPoolMemoryType.Java,
                PageFactory.CachingPolicy.PinnableLru,
                PageFactory.PageDescriptorType.Unsynchronized, retryLimit);
        pm = pf.createPageManager(null, pageSize, 3 * pages);
        pm.createPageSequence(name);
        pioS = new PageScan(null, pm, name);
        pioU = new PageUpdate(null, pm, name);
        pioA = new PageAppend(null, pm, name);
    }

    void doScan() {
        byte[] expected = new byte[pageSize];
        byte[] actual = new byte[pageSize];
        rand.setSeed(42);

        for (int i = 0, offset = 0; i < pages; i++, offset += pageSize) {
            rand.nextBytes(expected);
            int amountScanned = pioS.scan(actual, 0, pageSize, offset);
            assertEquals(pageSize, amountScanned);
            assertArrayEquals(expected, actual);
        }
    }

    void loadData() {
        Page page = pm.page(name);
        List<Page> ps = pm.getPageSequence(name);
        rand.setSeed(42);
        byte[] data = new byte[pageSize];

        for (int i = 0; i < pages; i++) {
            rand.nextBytes(data);
            page.write(data, 0, 0, data.length);
            page.setLimit(data.length);
            ps.add(page);
            page = pm.page(name);
        }
    }

    @Test
    public void testScan() {
        loadData();
        doScan();
    }
    
    @Test
    public void testPredicate() {
        IntegerValue iv = new IntegerValue(37);
        IntegerValue iv1 = new IntegerValue(34);
        AtomicPredicate ap = new AtomicPredicate(Operator.LESS_THAN, iv);
        AtomicPredicate ap1 = new AtomicPredicate(Operator.GREATER_THAN_EQUAL, iv1);
        CompoundPredicate cp = new CompoundPredicate(BinaryConnector.Conjunction, ap, ap1);
        
        Page page = pm.page(name);
        List<Page> ps = pm.getPageSequence(name);
        ps.add(page);
        
        byte[] data = new byte[4*5];
        Codec.getCodec().encode(37, data, 0);
        Codec.getCodec().encode(36, data, 4);
        Codec.getCodec().encode(35, data, 8);
        Codec.getCodec().encode(34, data, 12);
        Codec.getCodec().encode(33, data, 16);
        pioA.append(data, 0, data.length);
        pioS = new PageScan(cp, pm, name);
        
        byte[] actual = new byte[4*5];
//      PageScan pioS = new PageScan(null, pm, name);
        int amount = pioS.scan(actual, 0, 4*5, 0);
        assertEquals(4*5, amount);
        int actualDecoded = Codec.getCodec().decodeInteger(actual, 0);
        assertEquals(37, actualDecoded);
        actualDecoded = Codec.getCodec().decodeInteger(actual, 4);
        assertEquals(36, actualDecoded);
        actualDecoded = Codec.getCodec().decodeInteger(actual, 8);
        assertEquals(35, actualDecoded);
        actualDecoded = Codec.getCodec().decodeInteger(actual, 12);
        assertEquals(34, actualDecoded);
        actualDecoded = Codec.getCodec().decodeInteger(actual, 16);
        assertEquals(33, actualDecoded);
    }
    
    @Test
    public void testSinglePageFixedLengthUpdate() {

        byte[] randomData = null;

        rand.setSeed(42);

        Page page = pm.page(name);
        List<Page> ps = pm.getPageSequence(name);
        ps.add(page);
//        PageAppend pioA = new PageAppend(null, pm, name);

        int size = page.size() / 4;
        byte[] zero = new byte[size];
        byte[] one = new byte[size];
        byte[] two = new byte[size];
        byte[] three = new byte[size];

        rand.nextBytes(zero);
        rand.nextBytes(one);
        rand.nextBytes(two);
        rand.nextBytes(three);

        pioA.append(zero, 0, size);
        pioA.append(one, 0, size);
        pioA.append(two, 0, size);
        pioA.append(three, 0, size);

        assertEquals(page.size(), page.limit());

        randomData = new byte[size];
//        PageUpdate pioUpdate = new PageUpdate(null, pm, name);
        pioU.update(randomData, 0, size, size, 0);
        pioU.update(randomData, 0, size, size, 3 * size);

        byte[] actual = new byte[size];
//        PageScan pioS = new PageScan(null, pm, name);
        int amount = pioS.scan(actual, 0, size, 0);
        assertEquals(size, amount);
        assertArrayEquals(randomData, actual);

        amount = pioS.scan(actual, 0, size, size);
        assertEquals(size, amount);
        assertArrayEquals(one, actual);

        amount = pioS.scan(actual, 0, size, 2 * size);
        assertEquals(size, amount);
        assertArrayEquals(two, actual);

        amount = pioS.scan(actual, 0, size, 3 * size);
        assertEquals(size, amount);
        assertArrayEquals(randomData, actual);
    }

    @Test
    public void testVariableLengthSinglePageExpandFirstElement() {

        byte[] randomData = null;

        //PageAppend pio = new PageAppend(null, pm, name);
        rand.setSeed(42);

        Page page = pm.page(name);
        List<Page> ps = pm.getPageSequence(name);
        ps.add(page);

        int size = page.size() / 4;
        int newSize = page.size() / 3;

        byte[] zero = new byte[size];
        byte[] one = new byte[size];
        byte[] two = new byte[size];
        byte[] three = new byte[size];

        rand.nextBytes(zero);
        rand.nextBytes(one);
        rand.nextBytes(two);
        rand.nextBytes(three);

        pioA.append(zero, 0, size);
        pioA.append(one, 0, size);
        pioA.append(two, 0, size);
        pioA.append(three, 0, size);

        assertEquals(page.size(), page.limit());

        randomData = new byte[newSize];
        //PageUpdate pioU = new PageUpdate(null, pm, name);
        pioU.update(randomData, 0, size, newSize, 0);

        byte[] actual = new byte[newSize];
        //PageScan pioS = new PageScan(null, pm, name);
        int amount = pioS.scan(actual, 0, newSize, 0);
        assertEquals(newSize, amount);
        assertArrayEquals(randomData, actual);

        actual = new byte[size];
        amount = pioS.scan(actual, 0, size, newSize);
        assertEquals(size, amount);
        assertArrayEquals(one, actual);

        amount = pioS.scan(actual, 0, size, newSize + size);
        assertEquals(size, amount);
        assertArrayEquals(two, actual);

        amount = pioS.scan(actual, 0, size, newSize + 2 * size);
        assertEquals(size, amount);
        assertArrayEquals(three, actual);
    }

    @Test
    public void testVariableLengthSinglePageExpandLastElement() {

        byte[] randomData = null;

//        PageAppend pioA = new PageAppend(null, pm, name);
        rand.setSeed(42);

        Page page = pm.page(name);
        List<Page> ps = pm.getPageSequence(name);
        ps.add(page);

        int size = page.size() / 4;
        int newSize = page.size() / 3;

        byte[] zero = new byte[size];
        byte[] one = new byte[size];
        byte[] two = new byte[size];

        rand.nextBytes(zero);
        rand.nextBytes(one);
        rand.nextBytes(two);

        pioA.append(zero, 0, size);
        pioA.append(one, 0, size);
        pioA.append(two, 0, size);

        assertEquals(3 * page.size() / 4, page.limit());

        randomData = new byte[newSize];
  //      PageUpdate pioU = new PageUpdate(null, pm, name);
        pioU.update(randomData, 0, size, newSize, 2 * size);
        
        // assertEquals(2*size+newSize, page.limit());
        
        byte[] actual = new byte[size];
//        PageScan pioS = new PageScan(null, pm, name);
        int amount = pioS.scan(actual, 0, size, 0);
        assertEquals(size, amount);
        assertArrayEquals(zero, actual);

        amount = pioS.scan(actual, 0, size, size);
        assertEquals(size, amount);
        assertArrayEquals(one, actual);

        actual = new byte[newSize];
        amount = pioS.scan(actual, 0, newSize, 2 * size);
        assertEquals(newSize, amount);
        assertArrayEquals(randomData, actual);
    }

    @Test
    public void testVariableLengthSinglePageExpandInteriorElement() {

        byte[] randomData = null;

  //      PageAppend pioA = new PageAppend(null, pm, name);
        rand.setSeed(42);

        Page page = pm.page(name);
        List<Page> ps = pm.getPageSequence(name);
        ps.add(page);

        int size = page.size() / 4;
        int newSize = page.size() / 3;

        byte[] zero = new byte[size];
        byte[] one = new byte[size];
        byte[] two = new byte[size];

        rand.nextBytes(zero);
        rand.nextBytes(one);
        rand.nextBytes(two);

        pioA.append(zero, 0, size);
        pioA.append(one, 0, size);
        pioA.append(two, 0, size);
        

        assertEquals(3 * page.size() / 4, page.limit());

        randomData = new byte[newSize];
        pioU.update(randomData, 0, size, newSize, size);

        assertEquals(2 * size + newSize, page.limit());

        byte[] actual = new byte[size];
        int amount = pioS.scan(actual, 0, size, 0);
        assertEquals(size, amount);
        assertArrayEquals(zero, actual);

        amount = pioS.scan(actual, 0, size, newSize + size);
        assertEquals(size, amount);
        assertArrayEquals(two, actual);

        actual = new byte[newSize];
        amount = pioS.scan(actual, 0, newSize, size);
        assertEquals(newSize, amount);
        assertArrayEquals(randomData, actual);

    }

    @Test
    public void testVariableLength2PageExpandFirstElement() {

        byte[] randomData = null;

        rand.setSeed(42);

        Page page = pm.page(name);
        List<Page> ps = pm.getPageSequence(name);
        ps.add(page);

        int size = page.size() / 4;
        int newSize = page.size() / 3;

        byte[] zero = new byte[size];
        byte[] one = new byte[size];
        byte[] two = new byte[size];
        byte[] three = new byte[size];

        rand.nextBytes(zero);
        rand.nextBytes(one);
        rand.nextBytes(two);
        rand.nextBytes(three);

        pioA.append(zero, 0, size);
        pioA.append(one, 0, size);
        pioA.append(two, 0, size);
        pioA.append(three, 0, size);

        assertEquals(page.size(), page.limit());

        randomData = new byte[newSize];
        pioU.update(randomData, 0, size, newSize, 0);

        assertEquals(page.size(), page.limit());

        byte[] actual = new byte[newSize];
        int amount = pioS.scan(actual, 0, newSize, 0);
        assertEquals(newSize, amount);
        assertArrayEquals(randomData, actual);

        actual = new byte[size];
        amount = pioS.scan(actual, 0, size, newSize);
        assertEquals(size, amount);
        assertArrayEquals(one, actual);

        amount = pioS.scan(actual, 0, size, newSize + size);
        assertEquals(size, amount);
        assertArrayEquals(two, actual);

        amount = pioS.scan(actual, 0, size, newSize + 2 * size);
        assertEquals(size, amount);
        assertArrayEquals(three, actual);
    }

    @Test
    public void testVariableLength2PageExpandLastElement() {

        byte[] randomData = null;

//        pio = new PageState(pm, name);
        rand.setSeed(42);

        Page page = pm.page(name);
        List<Page> ps = pm.getPageSequence(name);
        ps.add(page);

        int size = page.size() / 4;
        int newSize = page.size() / 3;

        byte[] zero = new byte[size];
        byte[] one = new byte[size];
        byte[] two = new byte[size];
        byte[] three = new byte[size];

        rand.nextBytes(zero);
        rand.nextBytes(one);
        rand.nextBytes(two);
        rand.nextBytes(three);

        pioA.append(zero, 0, size);
        pioA.append(one, 0, size);
        pioA.append(two, 0, size);
        pioA.append(three, 0, size);

        assertEquals(page.size(), page.limit());

        randomData = new byte[newSize];
        pioU.update(randomData, 0, size, newSize, 3 * size);

        assertEquals(page.size(), page.limit());

        byte[] actual = new byte[newSize];
        int amount = pioS.scan(actual, 0, newSize, 3 * size);
        assertEquals(newSize, amount);
        assertArrayEquals(randomData, actual);

        actual = new byte[size];
        amount = pioS.scan(actual, 0, size, 0);
        assertEquals(size, amount);
        assertArrayEquals(zero, actual);

        amount = pioS.scan(actual, 0, size, size);
        assertEquals(size, amount);
        assertArrayEquals(one, actual);

        amount = pioS.scan(actual, 0, size, 2 * size);
        assertEquals(size, amount);
        assertArrayEquals(two, actual);
    }

    @Test
    public void testVariableLength2PageExpandInteriorElement() {

        byte[] randomData = null;

      //  pio = new PageState(pm, name);
        rand.setSeed(42);

        Page page = pm.page(name);
        List<Page> ps = pm.getPageSequence(name);
        ps.add(page);

        int size = page.size() / 4;
        int newSize = page.size() / 3;

        byte[] zero = new byte[size];
        byte[] one = new byte[size];
        byte[] two = new byte[size];
        byte[] three = new byte[size];

        rand.nextBytes(zero);
        rand.nextBytes(one);
        rand.nextBytes(two);
        rand.nextBytes(three);

        pioA.append(zero, 0, size);
        pioA.append(one, 0, size);
        pioA.append(two, 0, size);
        pioA.append(three, 0, size);

        assertEquals(page.size(), page.limit());

        randomData = new byte[newSize];
        pioU.update(randomData, 0, size, newSize, 2 * size);

        assertEquals(page.size(), page.limit());

        byte[] actual = new byte[newSize];
        int amount = pioS.scan(actual, 0, newSize, 2 * size);
        assertEquals(newSize, amount);
        assertArrayEquals(randomData, actual);

        actual = new byte[size];
        amount = pioS.scan(actual, 0, size, 0);
        assertEquals(size, amount);
        assertArrayEquals(zero, actual);

        amount = pioS.scan(actual, 0, size, size);
        assertEquals(size, amount);
        assertArrayEquals(one, actual);

        amount = pioS.scan(actual, 0, size, 2 * size + newSize);
        assertEquals(size, amount);
        assertArrayEquals(three, actual);

    }

    @Test
    public void testVariableLengthSinglePageShrinkFirstElement() {

        byte[] randomData = null;

    //    pio = new PageState(pm, name);
        rand.setSeed(42);

        Page page = pm.page(name);
        List<Page> ps = pm.getPageSequence(name);
        ps.add(page);

        int size = page.size() / 4;
        int newSize = page.size() / 5;

        byte[] zero = new byte[size];
        byte[] one = new byte[size];
        byte[] two = new byte[size];
        byte[] three = new byte[size];

        rand.nextBytes(zero);
        rand.nextBytes(one);
        rand.nextBytes(two);
        rand.nextBytes(three);

        pioA.append(zero, 0, size);
        pioA.append(one, 0, size);
        pioA.append(two, 0, size);
        pioA.append(three, 0, size);

        assertEquals(page.size(), page.limit());

        randomData = new byte[newSize];
        pioU.update(randomData, 0, size, newSize, 0);

        byte[] actual = new byte[newSize];
        int amount = pioS.scan(actual, 0, newSize, 0);
        assertEquals(newSize, amount);
        assertArrayEquals(randomData, actual);

        actual = new byte[size];
        amount = pioS.scan(actual, 0, size, newSize);
        assertEquals(size, amount);
        assertArrayEquals(one, actual);

        amount = pioS.scan(actual, 0, size, newSize + size);
        assertEquals(size, amount);
        assertArrayEquals(two, actual);

        amount = pioS.scan(actual, 0, size, newSize + 2 * size);
        assertEquals(size, amount);
        assertArrayEquals(three, actual);
    }

    @Test
    public void testVariableLengthSinglePageShrinkLastElement() {

        byte[] randomData = null;

  //      pio = new PageState(pm, name);
        rand.setSeed(42);

        Page page = pm.page(name);
        List<Page> ps = pm.getPageSequence(name);
        ps.add(page);

        int size = page.size() / 4;
        int newSize = page.size() / 5;

        byte[] zero = new byte[size];
        byte[] one = new byte[size];
        byte[] two = new byte[size];
        byte[] three = new byte[size];

        rand.nextBytes(zero);
        rand.nextBytes(one);
        rand.nextBytes(two);
        rand.nextBytes(three);

        pioA.append(zero, 0, size);
        pioA.append(one, 0, size);
        pioA.append(two, 0, size);
        pioA.append(three, 0, size);

        assertEquals(page.size(), page.limit());

        randomData = new byte[newSize];
        pioU.update(randomData, 0, size, newSize, 3 * size);

        byte[] actual = new byte[newSize];
        int amount = pioS.scan(actual, 0, newSize, 3 * size);
        assertEquals(newSize, amount);
        assertArrayEquals(randomData, actual);

        actual = new byte[size];
        amount = pioS.scan(actual, 0, size, size);
        assertEquals(size, amount);
        assertArrayEquals(one, actual);

        amount = pioS.scan(actual, 0, size, 2 * size);
        assertEquals(size, amount);
        assertArrayEquals(two, actual);

        amount = pioS.scan(actual, 0, size, 0);
        assertEquals(size, amount);
        assertArrayEquals(zero, actual);
    }

    @Test
    public void testVariableLengthSinglePageShrinkInteriorElement() {

        byte[] randomData = null;

//        pio = new PageState(pm, name);
        rand.setSeed(42);

        Page page = pm.page(name);
        List<Page> ps = pm.getPageSequence(name);
        ps.add(page);

        int size = page.size() / 4;
        int newSize = page.size() / 5;

        byte[] zero = new byte[size];
        byte[] one = new byte[size];
        byte[] two = new byte[size];
        byte[] three = new byte[size];

        rand.nextBytes(zero);
        rand.nextBytes(one);
        rand.nextBytes(two);
        rand.nextBytes(three);

        pioA.append(zero, 0, size);
        pioA.append(one, 0, size);
        pioA.append(two, 0, size);
        pioA.append(three, 0, size);

        assertEquals(page.size(), page.limit());

        randomData = new byte[newSize];
        pioU.update(randomData, 0, size, newSize, 2 * size);

        byte[] actual = new byte[newSize];
        int amount = pioS.scan(actual, 0, newSize, 2 * size);
        assertEquals(newSize, amount);
        assertArrayEquals(randomData, actual);

        actual = new byte[size];
        amount = pioS.scan(actual, 0, size, size);
        assertEquals(size, amount);
        assertArrayEquals(one, actual);

        amount = pioS.scan(actual, 0, size, 0);
        assertEquals(size, amount);
        assertArrayEquals(zero, actual);

        amount = pioS.scan(actual, 0, size, newSize + 2 * size);
        assertEquals(size, amount);
        assertArrayEquals(three, actual);
    }

    @Test
    public void testVariableLength2PageShrinkFirstElement() {

        byte[] randomData = null;

       // pio = new PageState(pm, name);
        rand.setSeed(42);

        Page page = pm.page(name);
        List<Page> ps = pm.getPageSequence(name);
        ps.add(page);

        int size = page.size() / 3;
        int newSize = page.size() / 4;

        byte[] zero = new byte[size];
        byte[] one = new byte[size];
        byte[] two = new byte[size];
        byte[] three = new byte[size];

        rand.nextBytes(zero);
        rand.nextBytes(one);
        rand.nextBytes(two);
        rand.nextBytes(three);

        pioA.append(zero, 0, size);
        pioA.append(one, 0, size);
        pioA.append(two, 0, size);
        pioA.append(three, 0, size);

        assertEquals(page.size(), page.limit());

        randomData = new byte[newSize];
        pioU.update(randomData, 0, size, newSize, 0);

        byte[] actual = new byte[newSize];
        int amount = pioS.scan(actual, 0, newSize, 0);
        assertEquals(newSize, amount);
        assertArrayEquals(randomData, actual);

        actual = new byte[size];
        amount = pioS.scan(actual, 0, size, newSize);
        assertEquals(size, amount);
        assertArrayEquals(one, actual);

        amount = pioS.scan(actual, 0, size, newSize + size);
        assertEquals(size, amount);
        assertArrayEquals(two, actual);

        amount = pioS.scan(actual, 0, size, newSize + 2 * size);
        assertEquals(size, amount);
        assertArrayEquals(three, actual);
    }

    @Test
    public void testVariableLength2PageShrinkLastElement() {

        byte[] randomData = null;

     //   pio = new PageState(pm, name);
        rand.setSeed(42);

        Page page = pm.page(name);
        List<Page> ps = pm.getPageSequence(name);
        ps.add(page);

        int size = page.size() / 3;
        int newSize = page.size() / 4;

        byte[] zero = new byte[size];
        byte[] one = new byte[size];
        byte[] two = new byte[size];
        byte[] three = new byte[size];

        rand.nextBytes(zero);
        rand.nextBytes(one);
        rand.nextBytes(two);
        rand.nextBytes(three);

        pioA.append(zero, 0, size);
        pioA.append(one, 0, size);
        pioA.append(two, 0, size);
        pioA.append(three, 0, size);

        assertEquals(page.size(), page.limit());

        randomData = new byte[newSize];
        pioU.update(randomData, 0, size, newSize, 3 * size);

        byte[] actual = new byte[newSize];
        int amount = pioS.scan(actual, 0, newSize, 3 * size);
        assertEquals(newSize, amount);
        assertArrayEquals(randomData, actual);

        actual = new byte[size];
        amount = pioS.scan(actual, 0, size, size);
        assertEquals(size, amount);
        assertArrayEquals(one, actual);

        amount = pioS.scan(actual, 0, size, 2 * size);
        assertEquals(size, amount);
        assertArrayEquals(two, actual);

        amount = pioS.scan(actual, 0, size, 0);
        assertEquals(size, amount);
        assertArrayEquals(zero, actual);
    }

    @Test
    public void testVariableLength2PageShrinkInteriorElement() {

        byte[] randomData = null;

      //  pio = new PageState(pm, name);
        rand.setSeed(42);

        Page page = pm.page(name);
        List<Page> ps = pm.getPageSequence(name);
        ps.add(page);

        int size = page.size() / 3;
        int newSize = page.size() / 4;

        byte[] zero = new byte[size];
        byte[] one = new byte[size];
        byte[] two = new byte[size];
        byte[] three = new byte[size];

        rand.nextBytes(zero);
        rand.nextBytes(one);
        rand.nextBytes(two);
        rand.nextBytes(three);

        pioA.append(zero, 0, size);
        pioA.append(one, 0, size);
        pioA.append(two, 0, size);
        pioA.append(three, 0, size);

        assertEquals(page.size(), page.limit());

        randomData = new byte[newSize];
        pioU.update(randomData, 0, size, newSize, 2 * size);

        byte[] actual = new byte[newSize];
        int amount = pioS.scan(actual, 0, newSize, 2 * size);
        assertEquals(newSize, amount);
        assertArrayEquals(randomData, actual);

        actual = new byte[size];
        amount = pioS.scan(actual, 0, size, size);
        assertEquals(size, amount);
        assertArrayEquals(one, actual);

        amount = pioS.scan(actual, 0, size, 0);
        assertEquals(size, amount);
        assertArrayEquals(zero, actual);

        amount = pioS.scan(actual, 0, size, newSize + 2 * size);
        assertEquals(size, amount);
        assertArrayEquals(three, actual);
    }

    @Test
    public void testAppend() {
        byte[] data = new byte[pageSize];
        rand.setSeed(42);
        List<Page> pageList = pm.getPageSequence(name);
        Page firstpage = pm.page(name);
        pageList.add(firstpage);
        //pio = new PageState(pm, name);
        for (int i = 0; i < pages; i++) {
            rand.nextBytes(data);
            pioA.append(data, 0, pageSize);
        }
        doScan();
    }

    enum TestState {
        Fixed, Shrink, Expand, All
    };

    @Test
    public void testUpdateFixedLength() {
        int max = 8 * pageSize;
        int iterations = 400;
        int randBound = 4;
        int randThreshold = 2;
        int seed = 1;
        doUpdateTest(max, iterations, randBound, randThreshold,
                TestState.Fixed, seed);
    }

    @Test
    public void testUpdateShrink() {
        int max = 8 * pageSize;
        int iterations = 400;
        int randBound = 4;
        int randThreshold = 2;
        int seed = 2;
        doUpdateTest(max, iterations, randBound, randThreshold,
                TestState.Shrink, seed);
    }

    @Test
    public void testUpdateExpand() {
        int max = 8 * pageSize;
        int iterations = 400;
        int randBound = 4;
        int randThreshold = 2;
        int seed = 31337;
        doUpdateTest(max, iterations, randBound, randThreshold,
                TestState.Expand, seed);
    }

    @Test
    public void testUpdateAll() throws Exception {
        int max = 8 * pageSize;
        int iterations = 400;
        int randBound = 4;
        int randThreshold = 2;
        int seed = 42;

        doUpdateTest(max, iterations, randBound, randThreshold, TestState.All,
                seed);
        tearDown();

        pageSize = 4095;
        pages = 4095;
        max = 8 * pageSize;
        tearDown();
        setupHelper();
        doUpdateTest(max, iterations, randBound, randThreshold, TestState.All,
                seed * seed);

        pageSize = 3113;
        pages = 5555;
        max = 8 * pageSize;
        tearDown();
        setupHelper();
        iterations = 1337;
        doUpdateTest(max, iterations, randBound, randThreshold, TestState.All,
                seed * seed + seed);
        /*
         * pageSize = 369; pages = 50000; max = 17*pageSize; tearDown();
         * setupHelper(); iterations = 21337; doUpdateTest(max, iterations,
         * randBound, randThreshold, TestState.All, seed*seed+seed*seed);
         */
    }

    public void doUpdateTest(int max, int iterations, int randBound,
            int randThreshold, TestState state, int seed) {
        HashMap<Integer, byte[]> data = new HashMap<Integer, byte[]>();

//        pio = new PageState(pm, name);
        rand.setSeed(seed);

        Page page = pm.page(name);
        List<Page> ps = pm.getPageSequence(name);
        ps.add(page);
        System.out.println("max" + max);
        for (int i = 0; i < iterations; i++) {
            byte[] next = new byte[rand.nextInt(max)];
            rand.nextBytes(next);
            pioA.append(next, 0, next.length);
            data.put(i, next);
            // System.out.println("writing index"+i+" len "+next.length);
        }

        long fileOffset = 0;

        for (int i = 0; i < iterations; i++) {
            byte[] expected = data.get(i);
            byte[] actual = new byte[expected.length];
            int amount = pioS.scan(actual, 0, actual.length, fileOffset);
            assertEquals(actual.length, amount);
            assertEquals(expected.length, amount);
            fileOffset += amount;
            assertArrayEquals(expected, actual);
        }

        for (int i = 0; i < iterations; i++) {
            int randValue = rand.nextInt(randBound);
            if (randValue < randThreshold) {
                int index = rand.nextInt(iterations);
                byte[] next = null;
                switch (state) {
                case All:
                    next = new byte[rand.nextInt(max)];
                    break;
                case Shrink:
                    next = new byte[rand.nextInt(data.get(index).length)];
                    break;
                case Expand:
                    int size = 0;
                    while (size < data.get(index).length)
                        size = rand.nextInt(max);

                    next = new byte[size];
                    break;
                case Fixed:
                    next = new byte[data.get(index).length];
                    break;
                default:
                    assert false;
                }

                long offset = 0;
                for (int j = 0; j < index; j++) {
                    offset += data.get(j).length;
                }

                int oldLen = data.get(index).length;
                data.put(index, next);
                // System.out.println(" ######################################################## Updated "+index+" iteration "+i+" len = "+next.length+","+oldLen
                // +" offset"+offset+" difference "+(next.length - oldLen)
                // +" pages "+(next.length - oldLen)/pageSize);

                pioU.update(next, 0, oldLen, next.length, offset);
                offset += next.length;
                for (int j = index + 1; j <= index + 1; j++) {
                    if (!(index + 1 < iterations)) {
                        break;
                    }
                    byte[] expected = data.get(j);
                    byte[] actual = new byte[expected.length];
                    // System.out.println("checking "+(j)+" with len = "+expected.length
                    // +" offset"+offset+" in pages "+(expected.length/pageSize));
                    int amount = pioS.scan(actual, 0, actual.length, offset);
                    assertEquals(actual.length, amount);
                    assertEquals(expected.length, amount);
                    // System.out.println("iteration "+i
                    // +" file offset "+fileOffset+" expected len = "+expected.length);
                    // fileOffset += amount;
                    assertArrayEquals(expected, actual);
                    offset += expected.length;
                }
            }
        }

        fileOffset = 0;

        for (int i = 0; i < iterations; i++) {
            byte[] expected = data.get(i);
            byte[] actual = new byte[expected.length];
            int amount = pioS.scan(actual, 0, actual.length, fileOffset);
            assertEquals(actual.length, amount);
            assertEquals(expected.length, amount);
            // System.out.println("iteration "+i
            // +" file offset "+fileOffset+" expected len = "+expected.length);
            fileOffset += amount;
            assertArrayEquals(expected, actual);
        }
    }
}