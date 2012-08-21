package syndeticlogic.catena.store;

import static org.junit.Assert.*;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import syndeticlogic.catena.codec.Codec;
import syndeticlogic.catena.stubs.PageManagerStub;
import syndeticlogic.catena.type.Type;

public class SegmentHeaderTest {
     PageManager pageManager;
     FileChannel fileChannel;
     SegmentHeader segmentHeader;
     String filename = "target"+System.getProperty("file.separator")+"segmentHeaderTest";
     
    @Before
    public void setUp() throws Exception {
        pageManager = new PageManagerStub();
        
        RandomAccessFile file = new RandomAccessFile(filename, "rw");
        fileChannel = file.getChannel();
        fileChannel.truncate(0);
        segmentHeader = new SegmentHeader(fileChannel, pageManager);
        Codec.configureCodec(null);
    }

    @After
    public void tearDown() throws Exception {
        fileChannel.truncate(0);
        fileChannel.force(true);
        pageManager = null;
        segmentHeader = null;
        fileChannel = null;
    }

    @Test
    public void testSegmentHeader() throws Exception {
        segmentHeader.type(Type.BINARY);
        segmentHeader.dataSize(22L);
        segmentHeader.pages(44);
        segmentHeader.store();
        
        fileChannel.force(true);
        fileChannel.close();
        RandomAccessFile file = new RandomAccessFile(filename, "rw");
        fileChannel = file.getChannel();
        segmentHeader = new SegmentHeader(fileChannel, pageManager);
        
        segmentHeader.load();
        assertEquals(Type.BINARY, segmentHeader.type());
        assertEquals(22L, segmentHeader.dataSize());
        assertEquals(44, segmentHeader.pages());
    }
}
