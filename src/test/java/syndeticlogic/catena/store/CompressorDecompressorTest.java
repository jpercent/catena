package syndeticlogic.catena.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.junit.Assert.*;

import org.junit.Test;

import syndeticlogic.catena.stubs.PageManagerStub;

import org.xerial.snappy.Snappy;

import syndeticlogic.catena.store.Compressor;
import syndeticlogic.catena.store.Decompressor;
import syndeticlogic.catena.store.NullCompressor;
import syndeticlogic.catena.store.NullDecompressor;
import syndeticlogic.catena.store.Page;
import syndeticlogic.catena.store.SnappyCompressor;
import syndeticlogic.catena.store.SnappyDecompressor;
import syndeticlogic.catena.utility.ArrayGenerator;
import syndeticlogic.catena.utility.FixedLengthArrayGenerator;

public class CompressorDecompressorTest {

    @Test
    public void testFullPage() throws Exception {
        testFullPage(true);
        testFullPage(false);
    }

    public void testFullPage(boolean snappy) throws Exception {
        int seed = 31;
        int pageSize = 4096;
        int elements = 1;
        int elementSize = 4096;

        ArrayGenerator fagen = new FixedLengthArrayGenerator(seed, elementSize,
                elements);
        ArrayList<byte[]> array = fagen.generateMemoryArray(elements);

        PageManagerStub pageManager = new PageManagerStub();
        pageManager.pageSize = pageSize * 2;
        Compressor bcompressor = null;

        if (snappy)
            bcompressor = new SnappyCompressor(pageManager, pageSize);
        else
            bcompressor = new NullCompressor(pageManager, pageSize);

        Page page = new Page();
        page.attachBuffer(ByteBuffer.allocateDirect(pageSize));
        page.write(array.get(0), 0, 0, pageSize);
        bcompressor.add(0, page);
        bcompressor.run();
        if (snappy)
            assertTrue(Snappy.isValidCompressedBuffer(bcompressor.getTarget()));

        Decompressor bdecompressor = null;
        if (snappy)
            bdecompressor = new SnappyDecompressor(page,
                    bcompressor.getTarget(), page.size());
        else
            bdecompressor = new NullDecompressor(page, bcompressor.getTarget());

        bdecompressor.run();
        byte[] bytes = new byte[pageSize];
        page.read(bytes, 0, 0, pageSize);
        assertArrayEquals(array.get(0), bytes);
        bcompressor = null;

        int count = 0;
        while (count < 31337) {
            System.gc();
            if (pageManager.unreleasedBuffers == 0)
                break;
            else
                Thread.sleep(10);

            count++;
        }
        assertTrue(count < 31337);
    }

    @Test
    public void testPartialPage0() throws IOException {
        testPartialPage0(true);
        testPartialPage0(false);
    }

    public void testPartialPage0(boolean snappy) throws IOException {
        int seed = 31;
        int pageSize = 4096;
        int elements = 1;
        int elementSize = 4096;

        ArrayGenerator fagen = new FixedLengthArrayGenerator(seed, elementSize,
                elements);
        ArrayList<byte[]> array = fagen.generateMemoryArray(elements);

        PageManagerStub pageManager = new PageManagerStub();
        pageManager.pageSize = pageSize * 2;

        Compressor bcompressor = null;

        if (snappy)
            bcompressor = new SnappyCompressor(pageManager, pageSize);
        else
            bcompressor = new NullCompressor(pageManager, pageSize);

        Page page = new Page();
        Page page1 = new Page();

        page.attachBuffer(ByteBuffer.allocateDirect(pageSize));
        page1.attachBuffer(ByteBuffer.allocateDirect(pageSize));

        page.write(array.get(0), 0, 0, pageSize / 2);
        page.setLimit(pageSize / 2);
        page1.write(array.get(0), pageSize / 2, pageSize / 2, pageSize / 2);
        bcompressor.add(0, page);
        bcompressor.add(pageSize / 2, page1);

        bcompressor.run();
        if (snappy)
            assertTrue(Snappy.isValidCompressedBuffer(bcompressor.getTarget()));
        Decompressor bdecompressor = null;
        if (snappy)
            bdecompressor = new SnappyDecompressor(page,
                    bcompressor.getTarget(), page.size());
        else
            bdecompressor = new NullDecompressor(page, bcompressor.getTarget());
        bdecompressor.run();
        byte[] bytes = new byte[pageSize];
        page.read(bytes, 0, 0, pageSize);
        assertArrayEquals(array.get(0), bytes);
        bcompressor.releaseTarget();
        assertEquals(0, pageManager.unreleasedBuffers);
    }

    @Test
    public void testPartialPage1() {
        testPartialPage1(true);
        testPartialPage1(false);
    }

    public void testPartialPage1(boolean snappy) {
        int seed = 31;
        int pageSize = 4096;
        int elements = 1;
        int elementSize = 4096;

        ArrayGenerator fagen = new FixedLengthArrayGenerator(seed, elementSize,
                elements);
        ArrayList<byte[]> array = fagen.generateMemoryArray(elements);

        PageManagerStub pageManager = new PageManagerStub();
        pageManager.pageSize = pageSize * 2;

        Compressor bcompressor = null;
        if (snappy)
            bcompressor = new SnappyCompressor(pageManager, pageSize);
        else
            bcompressor = new NullCompressor(pageManager, pageSize);

        Page page = new Page();
        Page page1 = new Page();

        page.attachBuffer(ByteBuffer.allocateDirect(pageSize));
        page1.attachBuffer(ByteBuffer.allocateDirect(pageSize));

        page.write(array.get(0), 0, pageSize / 2, pageSize / 2);
        page.setLimit(pageSize);
        page1.write(array.get(0), pageSize / 2, 0, pageSize / 2);
        page1.setLimit(pageSize / 2);
        bcompressor.add(pageSize / 2, page);
        bcompressor.add(0, page1);

        bcompressor.run();
        Decompressor bdecompressor = null;
        if (snappy)
            bdecompressor = new SnappyDecompressor(page,
                    bcompressor.getTarget(), page.size());
        else
            bdecompressor = new NullDecompressor(page, bcompressor.getTarget());

        bdecompressor.run();
        byte[] bytes = new byte[pageSize];
        page.read(bytes, 0, 0, pageSize);
        assertArrayEquals(array.get(0), bytes);
    }

    @Test
    public void testPartialPage2() throws IOException {
        int seed = 31;
        int pageSize = 4096;
        int elements = 1;
        int elementSize = 4096;

        ArrayGenerator fagen = new FixedLengthArrayGenerator(seed, elementSize,
                elements);
        ArrayList<byte[]> array = fagen.generateMemoryArray(elements);

        PageManagerStub pageManager = new PageManagerStub();
        pageManager.pageSize = pageSize;
        SnappyCompressor bcompressor = new SnappyCompressor(pageManager,
                pageSize);

        Page page = new Page();
        Page page1 = new Page();

        page.attachBuffer(ByteBuffer.allocateDirect(pageSize));
        page1.attachBuffer(ByteBuffer.allocateDirect(pageSize));

        page.write(array.get(0), 0, 0, 333);
        page.setLimit(333);
        page1.write(array.get(0), 333, 0, pageSize - 333);
        page1.setLimit(pageSize - 333);
        bcompressor.add(0, page);
        bcompressor.add(0, page1);
        bcompressor.run();
        assertTrue(Snappy.isValidCompressedBuffer(bcompressor.getTarget()));

        SnappyDecompressor bdecompressor = new SnappyDecompressor(page,
                bcompressor.getTarget(), page.size());
        bdecompressor.run();
        byte[] bytes = new byte[pageSize];
        page.read(bytes, 0, 0, pageSize);
        assertArrayEquals(array.get(0), bytes);
    }
}
