package syndeticlogic.catena.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.codec.Codec;
import syndeticlogic.catena.type.Type;

public class SerializedObjectChannel {
    private static Log log = LogFactory.getLog(SerializedObjectChannel.class);
    protected FileChannel channel;
    protected ByteBuffer length;
    protected Codec coder;
    
    public SerializedObjectChannel(FileChannel channel) {
        this.channel = channel;
        this.length = ByteBuffer.allocate(Type.INTEGER.length());
        this.coder = Codec.getCodec();
    }
    
    public void position(long position) {
        try {
            channel.position(position);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void force(boolean forceValue) {
        try {
            channel.force(forceValue);
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }
    
    public long position() {
        try {
            return channel.position();
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }
    
    public void skip() {
        try {
            int bytesRead = channel.read(length);
            assert bytesRead == Type.INTEGER.length();
            length.rewind();
            int objectSize = coder.decodeInteger(length);
            channel.position(channel.position() + objectSize);
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }
    
    public int read(ByteBuffer target) {
        try {
            int bytesRead = channel.read(length);
            assert bytesRead == Type.INTEGER.length();
            length.rewind();
            int object_size = coder.decodeInteger(length);
            length.rewind();
            assert target.capacity() >= target.position() + object_size;
            target.limit(target.position()+object_size);
            bytesRead = channel.read(target);
            assert bytesRead == object_size;
            return Type.INTEGER.length() + bytesRead;
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    public void writeHeader(PageManager pageManager, long dataSize, int pages) {
        try {
            channel.position(0L);
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
        SegmentHeader header = new SegmentHeader(channel, pageManager);
        header.load();
        header.dataSize(dataSize);
        header.pages(pages);
        header.store();
    }
    
    public long size() {
        try {
            return channel.size();
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }
    
    public void truncate(long offset) {
        try {
            this.channel = channel.truncate(offset);
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }
    
    public int write(ByteBuffer source) {
        coder.encode(source.remaining(), length);
        length.rewind();
        try {
            int bytesWritten = channel.write(length);
            assert bytesWritten == Type.INTEGER.length();            
            length.rewind();
            return channel.write(source);
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }
    
    public void complete() throws IOException {
        force(true);
    }
}

