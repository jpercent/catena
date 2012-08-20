package syndeticlogic.catena.store;

import static org.junit.Assert.*;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import org.junit.Test;
import org.xerial.snappy.Snappy;

import syndeticlogic.catena.store.SerializedObjectChannel;

public class SerializedObjectChannelTest {

    @Test
    public void test() throws Exception {
        RandomAccessFile file = new RandomAccessFile("target"+System.getProperty("file.separator")+"SerializedObjectChannel", "rw");
        FileChannel channel = file.getChannel();

        int maxSize = 4096;
        ByteBuffer buffer = ByteBuffer.allocateDirect(Snappy.maxCompressedLength(maxSize));
        ByteBuffer buffer1 = ByteBuffer.allocateDirect(Snappy.maxCompressedLength(maxSize));
        byte[] copyBuffer = new byte[maxSize];
        byte[] copyBuffer1 = new byte[maxSize];

        Random r = new Random(42);
        r.nextBytes(copyBuffer);
        buffer.put(copyBuffer);
        buffer.rewind();

        SerializedObjectChannel subject = new SerializedObjectChannel(channel);
        subject.write(buffer);
        buffer.rewind();
        subject.force(true);
        
        file = new RandomAccessFile("target"+System.getProperty("file.separator")+"SerializedObjectChannel", "rw");
        channel = file.getChannel();
        subject = new SerializedObjectChannel(channel);

        subject.read(buffer1);
        buffer1.rewind();
        
        buffer1.get(copyBuffer1);
        assertArrayEquals(copyBuffer, copyBuffer1);

        
        buffer = ByteBuffer.allocate(Snappy.maxCompressedLength(maxSize));
        buffer1 = ByteBuffer.allocate(Snappy.maxCompressedLength(maxSize));
        copyBuffer = new byte[maxSize];
        copyBuffer1 = new byte[maxSize];

        r.nextBytes(copyBuffer);
        buffer.put(copyBuffer);
        buffer.rewind();
        
        file = new RandomAccessFile("target"+ System.getProperty("file.separator")+"SerializedObjectChannel", "rw");
        channel = file.getChannel();
        subject = new SerializedObjectChannel(channel);
        subject.write(buffer);
        subject.force(true);
        
        file = new RandomAccessFile("target"+ System.getProperty("file.separator")+"SerializedObjectChannel", "rw");
        channel = file.getChannel();
        subject = new SerializedObjectChannel(channel);
        subject.read(buffer1);
        buffer1.rewind();
        buffer1.get(copyBuffer1);
        assertArrayEquals(copyBuffer, copyBuffer1);
        
    }

}
