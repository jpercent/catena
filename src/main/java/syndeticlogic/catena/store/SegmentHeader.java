package syndeticlogic.catena.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.CodeHelper;
import syndeticlogic.catena.utility.Codec;

public class SegmentHeader {
    private static final Log log = LogFactory.getLog(SegmentHeader.class);
    private static final int HEADER_SIZE = 23;
    
    private FileChannel channel;
    private PageManager pageManager;
    private int pages;
    private Type type;
    private long dataSize;
    
    public SegmentHeader(FileChannel channel, PageManager pageManager) {
        this.channel = channel;
        this.dataSize = 0L;
        this.pageManager = pageManager;
    }

    public void load() {
        try {
            if(channel.size() < HEADER_SIZE) {
                return;
            }
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }
        
        ByteBuffer buff = pageManager.byteBuffer();
        try {
            CodeHelper coder = Codec.getCodec().coder();
            buff.limit(HEADER_SIZE);
            int bytesRead = 0;
            try {
                bytesRead = channel.read(buff);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            assert bytesRead == HEADER_SIZE;
            buff.rewind();
            List<Object> meta = coder.decode(buff, 4);

            type = (Type) meta.get(0);
            pages = ((Integer) meta.get(1)).intValue();
            dataSize = ((Long) meta.get(2)).longValue();
            long crc = ((Long) meta.get(3)).longValue();
            coder.reset();
            coder.append(type);
            coder.append(pages);
            coder.append(dataSize);

            if (log.isTraceEnabled())
                log.trace(" type " + type + " numPages " + pages);

            byte[] content = coder.encodeByteArray();
            CRC32 testcrc = new CRC32();
            testcrc.update(content, 0, content.length);
            long crcvalue = testcrc.getValue();
            
            assert crcvalue == crc;
            
        } finally {
            pageManager.releaseByteBuffer(buff);
        }
    }

    public void store() {
        CodeHelper coder = Codec.getCodec().coder();
        coder.append(type);
        coder.append(pages);
        coder.append(dataSize);
        
        byte[] content = coder.encodeByteArray();
        CRC32 crc = new CRC32();
        crc.update(content, 0, content.length);
        long crcvalue = crc.getValue();

        coder.append(crcvalue);
        ByteBuffer buffer = coder.encodeByteBuffer();
        buffer.rewind();

        if (log.isTraceEnabled())
            log.trace("bytes written: " + buffer.limit());
        
        try {
            channel.position(0L);
            channel.write(buffer);
            channel.force(true);
            channel.position(0L);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    
    public Type type() {
        return type;
    }
    
    public void type(Type type) {
        this.type = type;
    }
    
    public int pages() {
        return pages;
    }
    
    public void pages(int pages) {
        this.pages = pages;
    }
    
    public long dataSize() {
        return dataSize;
    }

    public void dataSize(long dataSize) {
       this.dataSize = dataSize; 
    }
    
    public int headerSize() {
        return HEADER_SIZE;
    }
}
