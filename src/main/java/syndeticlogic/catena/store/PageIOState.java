package syndeticlogic.catena.store;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.predicate.Predicate;

public abstract class PageIOState {
    protected static final Log log = LogFactory.getLog(PageIOState.class);
    protected final Predicate predicate;
    protected final PageManager pageManager;
    protected final List<Page> pages;
    protected final String id;

    protected Page page;
    protected Page endPage;
    protected int pageOffset;
    protected int endPageOffset;

    protected byte[] buffer;
    protected int bufferOffset;

    protected long logicalOffset;
    protected long endOffset;

    protected int length;
    protected int total;
    protected int remaining;
    protected int index;
    protected int endIndex;

    PageIOState(Predicate predicate, PageManager pageManager, String id) {
        this.predicate = predicate;
        this.pageManager = pageManager;
        this.id = id;
        this.pages = pageManager.getPageSequence(id);
    }

    protected void prepare(byte[] buffer, int bufferOffset, int length,
            long loffset) {
        this.page = null;
        this.pageOffset = -1;
        this.buffer = buffer;
        this.bufferOffset = bufferOffset;
        this.logicalOffset = loffset;
        this.length = length;
        this.remaining = length;
        this.total = 0;
        this.index = 0;
    }


    protected void prepareAppend(byte[] buffer, int bufferOffset, int length) {
        prepare(buffer, bufferOffset, length, -1);
        page = pages.get(pages.size() - 1);
        pageOffset = page.limit();
    }

    protected void prepareScan(byte[] buffer, int bufferOffset, int length,
            long loffset) {
        prepare(buffer, bufferOffset, length, loffset);
        setIndexAndPageOffset();
    }

    protected void prepareUpdate(byte[] buffer, int bOffset, int oldlen,
            int newlen, long loffset) {
        prepare(buffer, bOffset, newlen, loffset);
        long cursor = setIndexAndPageOffset();

        this.endPage = null;
        this.endPageOffset = -1;
        this.endIndex = -1;
        this.endOffset = loffset + (long) oldlen;
        endIndex = index;
        Page pageIter = page;
        while (endIndex < pages.size()) {
            if (cursor + pageIter.limit() < endOffset) {
                cursor += pageIter.limit();
                endIndex++;
            } else if (cursor + pageIter.limit() == endOffset) {
                cursor += pageIter.limit();
                break;
            } else {
                break;
            }
            pageIter = pages.get(endIndex);
        }

        endPage = pages.get(endIndex);
        assert cursor <= endOffset;
        if (cursor != endOffset) {
            endPageOffset = (int) (endOffset - cursor);
        } else {
            endPageOffset = endPage.limit();
        }

        if (log.isDebugEnabled()) {
            log.debug("endindex = " + endIndex + " pages = " + pages.size()
                    +" endPageOffset = "+endPageOffset
                    +" enpage.limit() = "+endPage.limit());
        }
    }

    protected void advance(int iobytes) {
        assert pageOffset <= page.size();
        total += iobytes;
        bufferOffset += iobytes;
        pageOffset += iobytes;
        remaining -= iobytes;
    }
    
    protected long setIndexAndPageOffset() {
        long cursor = 0;

        for (Page pageIter : pages) {
            if (cursor + pageIter.limit() < logicalOffset) {
                cursor += pageIter.limit();
                index++;
            } else if (cursor + pageIter.limit() == logicalOffset) {
                cursor += pageIter.limit();
                index++;
                break;
            } else {
                break;
            }
        }

        assert cursor <= logicalOffset;
        pageOffset = (int) (logicalOffset - cursor);
        page = pages.get(index);
        
        if (log.isDebugEnabled()) {
            log.debug(" logicalOffset = " + logicalOffset + " cursor = " + cursor
                    + "index = " + index + "pages = " + pages.size() + "pageoffset = "
                    + pageOffset);
        }
        return cursor;
    }
    
    protected void complete() {
        this.page = null;
        this.endPage = null;
        this.pageOffset = -1;
        this.endPageOffset = -1;
        this.buffer = null;
        this.bufferOffset = -1;
        this.logicalOffset = -1;
        this.endOffset = -1;
        this.length = -1;
        this.total = -1;
        this.remaining = -1;
        this.index = -1;
        this.endIndex = -1;
    }
}
