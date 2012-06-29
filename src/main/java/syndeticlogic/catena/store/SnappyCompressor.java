package syndeticlogic.catena.store;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xerial.snappy.Snappy;

public class SnappyCompressor implements Compressor {

    private Log log = LogFactory.getLog(SnappyCompressor.class);
    private List<PageDescriptor> pages;
    private PageManager pageManager;
    private List<Integer> offsets;
    private ByteBuffer target;
    private int compressionSize;
    private boolean isDirect;

    public SnappyCompressor(PageManager pageManager, int compressionSize) {
        pages = new LinkedList<PageDescriptor>();
        offsets = new LinkedList<Integer>();
        isDirect = true;
        this.compressionSize = compressionSize;
        this.pageManager = pageManager;
    }

    public synchronized void add(int offset, PageDescriptor page) {
        pages.add(page);
        if (isDirect) {
            isDirect = page.isDirect();
        }
        offsets.add(offset);
    }

    public ByteBuffer coalese() {
        // XXX - need to figure out how to manage these
        ByteBuffer source = ByteBuffer.allocateDirect(pageManager.pageSize());
        source.limit(compressionSize);
        int total = 0;
        int offset = 0;
        int pageIndex = 0;

        while (total <= compressionSize) {

            PageDescriptor page = pages.get(pageIndex);
            offset = offsets.get(pageIndex);
            int size = page.limit() - offset;

            source.limit(source.position() + size);
            int bytesRead = page.read(source, offset);
            total += bytesRead;
            source.limit(compressionSize);
            pageIndex++;

            if (pageIndex == pages.size())
                break;
        }
        assert total <= compressionSize;
        return source;
    }

    public synchronized void run() {
        assert pages.size() == offsets.size();
        ByteBuffer source = null;
        boolean release = false;
        if (!isDirect || pages.size() > 1) {
            source = coalese();
        } else {
            release = true;
            source = pages.get(0).getBuffer();
        }
        source.rewind();
        assert source.limit() - source.position() == compressionSize;
        boolean compressed = true;

        target = ByteBuffer.allocateDirect(
                Snappy.maxCompressedLength(compressionSize));
        try {
            Snappy.compress(source, target);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (BufferOverflowException of) {
            log.debug("BufferOverflowException on page.filename = "
                    + pages.get(0).getDataSegmentId());
            compressed = false;
        } catch (IllegalArgumentException ia) {
            log.debug("Compression failed page.filename = "
                    + pages.get(0).getDataSegmentId());
            compressed = false;
        }

        if (!compressed) {
            ByteBuffer swap = source;
            source = target;
            target = swap;
        }

        if (!release) {
            source = null;
        }
    }

    public synchronized void releaseTarget() {
        target = null;
    }

    protected synchronized void finalize() throws Throwable {
        try {
            // if(target != null)
            // pageManager.releaseByteBuffer(target);

        } finally {
            super.finalize();
        }
    }

    public synchronized ByteBuffer getTarget() {
        return target;
    }

    @Override
    public Object call() throws Exception {
        run();
        return null;
    }
}
