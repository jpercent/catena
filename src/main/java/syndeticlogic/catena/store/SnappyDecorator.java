package syndeticlogic.catena.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xerial.snappy.Snappy;

public class SnappyDecorator extends SerializedObjectChannel {
    private Log log = LogFactory.getLog(SnappyDecorator.class);
    private ByteBuffer snappyBuffer;
    
    public SnappyDecorator(FileChannel channel, int maxSize, boolean useDirect) {
        super(channel);
        if(useDirect) {
            snappyBuffer = ByteBuffer.allocateDirect(Snappy.maxCompressedLength(maxSize));
        } else {
            snappyBuffer = ByteBuffer.allocate(Snappy.maxCompressedLength(maxSize));
        }
    }
    
    @Override
    public int write(ByteBuffer source) {
        ByteBuffer dataToStore = snappyBuffer;
        try {
            if (source.isDirect() && snappyBuffer.isDirect()) {
                Snappy.compress(source, snappyBuffer);
            } else if (!source.isDirect() && !snappyBuffer.isDirect()) {
                byte[] sourceArray = source.array();
                byte[] snappyBufferArray = snappyBuffer.array();
                int compressedBytes = Snappy.rawCompress(sourceArray, 0, sourceArray.length, snappyBufferArray, 0);
                snappyBuffer.limit(compressedBytes);
            } else {
                log.debug("buffer source memory type mismatch; not compressing");
                dataToStore = source;
            }
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
        
        dataToStore.rewind();
        int written = 0;
        written = super.write(dataToStore);
        snappyBuffer.limit(snappyBuffer.capacity());
        snappyBuffer.rewind();
        return written;
    }
    
    @Override
    public int read(ByteBuffer target) {
        int bytesRead = super.read(snappyBuffer);
        snappyBuffer.rewind();
        boolean uncompressed = false;
        try {
            if(snappyBuffer.isDirect() && target.isDirect()) {
                if(Snappy.isValidCompressedBuffer(snappyBuffer)) {
                    assert Snappy.uncompressedLength(snappyBuffer) <= target.limit();
                    bytesRead = Snappy.uncompress(snappyBuffer, target);
                    target.limit(bytesRead);
                    snappyBuffer.rewind();
                    snappyBuffer.limit(snappyBuffer.capacity());
                } else {
                    uncompressed = true;
                }
            } else if(!snappyBuffer.isDirect() && !target.isDirect()) {
                if(Snappy.isValidCompressedBuffer(snappyBuffer.array(), 0, snappyBuffer.remaining())) {
                    assert Snappy.uncompressedLength(snappyBuffer.array(), 0, snappyBuffer.remaining()) <= target.limit();
                    bytesRead = Snappy.uncompress(snappyBuffer.array(), 0, snappyBuffer.remaining(), target.array(), 0);
                    target.limit(bytesRead);
                    snappyBuffer.rewind();
                    snappyBuffer.limit(snappyBuffer.capacity());
                } else {    
                    uncompressed = true;
                }
            }
            
            if(uncompressed) {
                target.put(snappyBuffer);
                snappyBuffer.rewind();
                snappyBuffer.limit(snappyBuffer.capacity());
            }
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
        return bytesRead;
    }
}
