package syndeticlogic.catena.store;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NullCompressor implements Compressor {

    private Log log = LogFactory.getLog(NullCompressor.class);
    private List<PageDescriptor> pages;
    private PageManager pageManager;
    private List<Integer> offsets;
    private ByteBuffer target;
    private int compressionSize;
    private boolean isDirect;

    public NullCompressor(PageManager pageManager, int compressionSize) {
        pages = new LinkedList<PageDescriptor>();
        offsets = new LinkedList<Integer>();
        isDirect = true;
        this.compressionSize = compressionSize;
        this.pageManager = pageManager;
        if (log.isTraceEnabled())
            log.trace(" compressionSize = " + compressionSize);
    }

    public synchronized void add(int offset, PageDescriptor page) {
        pages.add(page);
        if (isDirect) {
            isDirect = page.isDirect();
        }
        offsets.add(offset);
    }

    public ByteBuffer coalese() {
        int total = 0;
        int offset = 0;
        int i = 0;
        while (total <= compressionSize) {
            PageDescriptor page = pages.get(i);
            offset = offsets.get(i);
            int size = page.limit() - offset;
            target.limit(target.position() + size);
            int bytesRead = page.read(target, offset);
            total += bytesRead;
            target.limit(compressionSize);
            i++;

            if (i == pages.size())
                break;
        }
        assert total == compressionSize;
        return target;
    }

    public synchronized void run() {
        assert pages.size() == offsets.size();
        target = pageManager.byteBuffer();
        target.limit(compressionSize);

        if (pages.size() > 1)
            coalese();
        else
            pages.get(0).read(target, 0);

        target.rewind();
        assert target.limit() - target.position() == compressionSize;
    }

    public synchronized void releaseTarget() {
        pageManager.releaseByteBuffer(target);
        target = null;
    }

    protected synchronized void finalize() throws Throwable {
        try {
            if (target != null)
                pageManager.releaseByteBuffer(target);

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
