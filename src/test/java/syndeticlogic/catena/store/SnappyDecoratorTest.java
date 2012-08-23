package syndeticlogic.catena.store;


import static org.junit.Assert.*;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import org.junit.Test;
import org.xerial.snappy.Snappy;

import syndeticlogic.catena.store.SnappyDecorator;
import syndeticlogic.catena.utility.Codec;

public class SnappyDecoratorTest {
    
    @Test
    public void test() throws Exception {
        RandomAccessFile file = new RandomAccessFile("target"+System.getProperty("file.separator")+"SnappyDecoratorTest", "rw");
        FileChannel channel = file.getChannel();
        Codec.configureCodec(null);
        int maxSize = 4096;
        ByteBuffer buffer = ByteBuffer.allocateDirect(Snappy.maxCompressedLength(maxSize));
        ByteBuffer buffer1 = ByteBuffer.allocateDirect(Snappy.maxCompressedLength(maxSize));
        byte[] copyBuffer = new byte[maxSize];
        byte[] copyBuffer1 = new byte[maxSize];

        Random r = new Random(42);
        r.nextBytes(copyBuffer);
        buffer.put(copyBuffer);
        buffer.rewind();

        SnappyDecorator subject = new SnappyDecorator(channel, maxSize, true);
        subject.write(buffer);
        buffer.rewind();
        
        file = new RandomAccessFile("target"+System.getProperty("file.separator")+"SnappyDecoratorTest", "rw");
        channel = file.getChannel();
        subject = new SnappyDecorator(channel, maxSize, true);

        subject.read(buffer1);
        buffer1.rewind();
        
        buffer1.get(copyBuffer1);
        assertArrayEquals(copyBuffer, copyBuffer1);
/*
        for(int i = 0; i < 1000; i ++) {
            System.out.print(copyBuffer[i]+",");
        }
        System.out.println();
        for(int i = 0; i < 1000; i ++) {
            System.out.print(copyBuffer1[i]+",");
        }
        System.out.println("");
  */      
        buffer = ByteBuffer.allocate(Snappy.maxCompressedLength(maxSize));
        buffer1 = ByteBuffer.allocate(Snappy.maxCompressedLength(maxSize));
        copyBuffer = new byte[maxSize];
        copyBuffer1 = new byte[maxSize];

        r.nextBytes(copyBuffer);
        buffer.put(copyBuffer);
        buffer.rewind();
        
        file = new RandomAccessFile("target"+ System.getProperty("file.separator")+"SnappyDecoratorTest", "rw");
        channel = file.getChannel();
        subject = new SnappyDecorator(channel, maxSize, false);
        subject.write(buffer);
        subject.force(true);
        
        file = new RandomAccessFile("target"+ System.getProperty("file.separator")+"SnappyDecoratorTest", "rw");
        channel = file.getChannel();
        subject = new SnappyDecorator(channel, maxSize, false);
        subject.read(buffer1);
        buffer1.rewind();
        buffer1.get(copyBuffer1);
    /*    
        for(int i = 0; i < 1000; i ++) {
            System.out.print(copyBuffer[i]+",");
        }
        System.out.println();
        for(int i = 0; i < 1000; i ++) {
            System.out.print(copyBuffer1[i]+",");
        }
      */  
        assertArrayEquals(copyBuffer, copyBuffer1);
        
    }
}
