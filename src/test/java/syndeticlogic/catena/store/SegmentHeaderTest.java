package syndeticlogic.catena.store;

import static org.junit.Assert.*;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.junit.After;
import org.junit.Test;

import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;

public class SegmentHeaderTest {
     
    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testSegmentHeader() throws Exception {
        PageFactory pageFactory = new PageFactory(PageFactory.BufferPoolMemoryType.Java,
                PageFactory.CachingPolicy.PinnableLru,
                PageFactory.PageDescriptorType.Unsynchronized, /* retryLimit */ 2);
        PageManager pageManager = pageFactory.createPageManager(null, /* pageSize */ 4096, 3 * /* pages */ 128);
     
        String filename = "target"+System.getProperty("file.separator")+"segmentHeaderTest";
        RandomAccessFile file = new RandomAccessFile(filename, "rw");
        FileChannel fileChannel = file.getChannel();
        fileChannel.truncate(0);
        SegmentHeader segmentHeader = new SegmentHeader(fileChannel, pageManager);
        Codec.configureCodec(null);
        
        segmentHeader.type(Type.BINARY);
        segmentHeader.dataSize(22);
        segmentHeader.pages(44);
        segmentHeader.store();
        
        fileChannel.force(true);
        fileChannel.close();
        file.close();
        
        file = new RandomAccessFile(filename, "rw");
        fileChannel = file.getChannel();
        segmentHeader = new SegmentHeader(fileChannel, pageManager);
        
        segmentHeader.load();
        assertEquals(Type.BINARY, segmentHeader.type());
        assertEquals(22L, segmentHeader.dataSize());
        assertEquals(44, segmentHeader.pages());

        fileChannel.truncate(0);
        fileChannel.force(true);
        pageManager = null;
        segmentHeader = null;
        fileChannel = null;
        file.close();
    }
}
