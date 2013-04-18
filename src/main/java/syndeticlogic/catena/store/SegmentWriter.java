package syndeticlogic.catena.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SegmentWriter {

    private static final Log log = LogFactory.getLog(SegmentReader.class);
    private SerializedObjectChannel channel;
    private PageManager pageManager;
    private HashMap<Integer, Long> pageOffsets;
    private String segmentId;
    
  public SegmentWriter(SerializedObjectChannel channel, PageManager pageManager, HashMap<Integer, 
            Long> pageOffsets, String segmentId) {
        this.channel = channel;
        this.pageManager = pageManager;
        this.pageOffsets = pageOffsets;
        this.segmentId = segmentId;
    }
    
    public void write() {
        long fileoffset = 0;
        int dirtyIndex = 0;
        List<Page> pages = pageManager.getPageSequence(segmentId);
        
        for (; dirtyIndex < pages.size(); dirtyIndex++) {
            fileoffset = pageOffsets.get(dirtyIndex);
            if (pages.get(dirtyIndex).isDirty()) {
                break;
            }
        }
                
        if (dirtyIndex == pages.size()) {
            return;
        }

        try {
            writeDirtyPages(pages, dirtyIndex, fileoffset);
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }
    
    private void writeDirtyPages(List<Page> pages, int dirtyIndex, long fileoffset) throws IOException {        
        channel.position(fileoffset);
        int total = 0;
        
        for(int i = dirtyIndex; i < pages.size(); i++) {
            pageOffsets.put(i, fileoffset);
            Page page = pages.get(i);
            ByteBuffer source = page.getBuffer();
            source.limit(page.limit());
            int bytesWritten = channel.write(source);
            source.rewind();
            total += source.remaining();
            fileoffset += bytesWritten;
        }
        channel.writeHeader(pageManager, total, pages.size());
     }
}
