package syndeticlogic.catena.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SegmentReader {
    private static final Log log = LogFactory.getLog(SegmentReader.class);
    private SegmentHeader segmentHeader;
    private SerializedObjectChannel channel;
    private PageManager pageManager;
    private List<Page> pages;
    private String segmentId;

    public SegmentReader(SegmentHeader header, SerializedObjectChannel channel, PageManager pageManager, 
            List<Page> pageVector, String segmentId) {
        this.segmentHeader = header;
        this.channel = channel;
        this.pageManager = pageManager;
        this.pages = pageVector;
        this.segmentId = segmentId;
    }

    public HashMap<Integer, Long> load() {
        HashMap<Integer, Long> pageOffsets = new HashMap<Integer, Long>();
        if (segmentHeader.pages() == 0) {
            assert pages.size() == 0;
            Page page = pageManager.page(segmentId);
            pages.add(page);
            pageOffsets.put(0, (long) segmentHeader.headerSize());
        } else {
            if (pages.size() == 0) {
                loadAllPages(pageOffsets);
            } else {
                loadUncachedPages(pageOffsets);
            }
            
            if (log.isTraceEnabled()) { 
                log.trace("pages.size() " + pages.size());
            }
        }
        return pageOffsets;
    }

    public void loadAllPages(HashMap<Integer, Long> pageOffsets) {
        try {
            channel.position(segmentHeader.headerSize());
            for (int index = 0; index < segmentHeader.pages(); index++) {
                pageOffsets.put(index, channel.position());
                Page page = pageManager.page(segmentId);
                pages.add(page);
                loadPage(page);
            }
            channel.position(0L);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void loadUncachedPages(HashMap<Integer, Long> pageOffsets) {
        try {
            channel.position(segmentHeader.headerSize());
            for (int index = 0; index < segmentHeader.pages(); index++) {
                pageOffsets.put(index, channel.position());
                Page page = pages.get(index);
                boolean loadPage = false;
                if (!page.isAssigned()) {
                    loadPage = true;
                } else {
                    channel.skip();
                }

                if (loadPage) {
                    loadPage(page);
                }
            }
            channel.position(0L);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void loadPage(Page page) throws IOException {
        channel.read(page.getBuffer());
        page.getBuffer().rewind();
    }
}