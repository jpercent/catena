package syndeticlogic.catena.store;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.xerial.snappy.Snappy;

public class SnappyDecompressor implements Decompressor {
    private ByteBuffer source;
    private PageDescriptor target;
    private int pageSize;

    public SnappyDecompressor(PageDescriptor target, ByteBuffer source, int pageSize) {
        this.target = target;
        this.source = source;
        this.pageSize = pageSize;
    }

    @Override
    public void run() {
        try {
            assert source != null;
            int iobytes = 0;
            if (Snappy.isValidCompressedBuffer(source)) {
                assert Snappy.uncompressedLength(source) <= target.size();
                ByteBuffer targetbuffer = null;
                if (!target.isDirect()) {
                    targetbuffer = ByteBuffer.allocateDirect(pageSize);
                } else {
                    targetbuffer = target.getBuffer();
                }
                iobytes = Snappy.uncompress(source, targetbuffer);
                if (!target.isDirect()) {
                    targetbuffer.rewind();
                    target.write(targetbuffer, 0);
                }
            } else {
                iobytes = target.write(source, 0);
            }
            target.setLimit(iobytes);

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("decompression failed");
        } finally {
        }
    }
    
    @Override
    public ByteBuffer source() {
        return source;
    }

    @Override
    public Object call() throws Exception {
        run();
        return null;
    }
}
