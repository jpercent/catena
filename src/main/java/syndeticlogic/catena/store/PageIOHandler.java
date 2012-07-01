package syndeticlogic.catena.store;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PageIOHandler {
    protected static final Log log = LogFactory.getLog(PageIOHandler.class);
    private final PageManager pageManager;
    private final List<PageDescriptor> pages;
    private final String id;

    private PageDescriptor page;
    private PageDescriptor endPage;
    private int pageOffset;
    private int endPageOffset;

    private byte[] buffer;
    private int bufferOffset;

    private long logicalOffset;
    private long endOffset;

    private int length;
    private int total;
    private int remaining;
    private int index;
    private int endIndex;

    PageIOHandler(PageManager pageManager, String id) {
        this.pageManager = pageManager;
        this.id = id;
        this.pages = pageManager.getPageSequence(id);
    }

    public int scan(byte[] buf, int bufOffset, int length, long foffset) {
        prepareScan(buf, bufOffset, length, foffset);

        if (pageOffset == 0 && page.limit() == 0) {
            return 0;
        }

        int iobytes = 0;
        while (total < length) {
            assert pageOffset < page.limit();
            int size = remaining;
            if (pageOffset + size > page.limit()) {
                size = page.limit() - pageOffset;
            }
            iobytes = page.read(buffer, bufferOffset, pageOffset, size);
            advance(iobytes);

            if (total < length) {
                index++;
                pageOffset = 0;
                assert index < pages.size();
                page = pages.get(index);
            }
        }
        int ret = total;
        complete();
        return ret;
    }

    public void update(byte[] buf, int bufOffset, int oldLen, int newLen,
            long loffset) {
        prepareUpdate(buf, bufOffset, oldLen, newLen, loffset);
        overwrite();
        assert (pageOffset == endPageOffset && endPage == page)
                || total == length;
        if (total < length) {
            // increase
            insert();
        } else if (endPage != page || endPageOffset != pageOffset) {
            // decrease
            truncate();
        }
        complete();
    }

    public void append(byte[] buf, int bufOffset, int size) {
        prepareAppend(buf, bufOffset, size);

        if (page.size() == page.limit()) {
            PageDescriptor newPage = pageManager.pageDescriptor(id);
            pages.add(newPage);
            page = newPage;
            pageOffset = 0;
        }

        int iobytes = 0;
        while (total < length) {
            iobytes = page.write(buffer, bufferOffset, pageOffset, remaining);
            advance(iobytes);
            page.setLimit(pageOffset);

            if (total < length) {
                index++;
                pageOffset = 0;
                page = pageManager.pageDescriptor(id);
                pages.add(page);
            }
        }
        complete();
    }

    private void prepareAppend(byte[] buffer, int bufferOffset, int length) {
        prepare(buffer, bufferOffset, length, -1);
        page = pages.get(pages.size() - 1);
        pageOffset = page.limit();
    }

    private void prepareScan(byte[] buffer, int bufferOffset, int length,
            long loffset) {
        prepare(buffer, bufferOffset, length, loffset);
        setIndexAndPageOffset();
    }

    private void prepareUpdate(byte[] buffer, int bOffset, int oldlen,
            int newlen, long loffset) {
        prepare(buffer, bOffset, newlen, loffset);
        long cursor = setIndexAndPageOffset();

        this.endPage = null;
        this.endPageOffset = -1;
        this.endIndex = -1;
        this.endOffset = loffset + (long) oldlen;
        endIndex = index;
        PageDescriptor pageIter = page;
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

        if (log.isTraceEnabled()) {
            log.trace("endindex" + endIndex + "pages" + pages.size()
                    +"endPageOffset "+endPageOffset
                    +"elimit() "+endPage.limit());
        }
    }

    private void prepare(byte[] buffer, int bufferOffset, int length,
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
    
    private void complete() {
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

    private long setIndexAndPageOffset() {
        long cursor = 0;

        for (PageDescriptor pageIter : pages) {
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
        
        if (log.isTraceEnabled()) {
            log.trace("logicalOffset" + logicalOffset + "cursor" + cursor
                    + "index" + index + "pages" + pages.size() + "pageoffset"
                    + pageOffset);
        }
        return cursor;
    }

    private void advance(int iobytes) {
        assert pageOffset <= page.size();
        total += iobytes;
        bufferOffset += iobytes;
        pageOffset += iobytes;
        remaining -= iobytes;
    }

    private void overwrite() {
        int iobytes = 0;
        boolean done = false;

        while (!done) {
            int size = remaining;
            if (endPage == page && pageOffset < endPageOffset) {
                if (remaining > endPageOffset - pageOffset) {
                    size = endPageOffset - pageOffset;
                    done = true;
                }
            } else if (endPage == page && pageOffset == endPageOffset) {
                break;
            }

            if (total >= length) {
                break;
            }
            
            if(log.isTraceEnabled()) {
                log.trace("OVERWRITE: total "+total+" leng "+length+" index "+index
                        +" spages "+pages.size()+" remaining"+remaining+" size "+size
                        +" pageOffset "+pageOffset+" page limit "+page.limit()
                        +" bufferOffset "+bufferOffset);
            }
            
            iobytes = page.write(buffer, bufferOffset, pageOffset, size);
            advance(iobytes);

            if (pageOffset > page.limit()) {
                page.setLimit(pageOffset);
            }

            if (total < length && !done) {
                index++;
                pageOffset = 0;
                assert index < pages.size();
                page = pages.get(index);
            }
        }
    }

    private void truncate() {
        if(log.isTraceEnabled()) {
            log.trace("endpage == page "+(endPage == page)
                    +" endoffset"+endPageOffset+" pageoffset"+pageOffset
                    +" endPage.limit() "+endPage.limit()
                    +" page.limit() "+page.limit());
        }
        
        int tail = endPage.limit() - endPageOffset;
        assert tail >= 0;
        if (endPage == page && tail == 0) {
            // no good data after teh endpageoffset
            page.setLimit(pageOffset);
            return;
        } else if (endPage == page) {
            // there's good data after teh endpageoffset.  since the pageOffset < endpageoffset, 
            // we know that the good data fits on this page, so we shift it up and return
            assert pageOffset < endPageOffset && tail > 0;
            int sz = endPage.limit() - endPageOffset;
            byte[] gooddata = new byte[sz];
            page.read(gooddata, 0, endPageOffset, tail);
            page.write(gooddata, 0, pageOffset, tail);
            page.setLimit(pageOffset + tail);
            return;
        } else {
            // otherwise set teh page limit.  there is a least 1 partial page
            // after page, that has dead data on it
            page.setLimit(pageOffset);
        }

        // create a list of the dead pages and then remove them
        index++;
        int deadindex = index;
        boolean done = false;
        List<PageDescriptor> deadpages = new LinkedList<PageDescriptor>();
        
        while (!done) {
            assert deadindex < pages.size();
            PageDescriptor deadpage = pages.get(deadindex);

            if ((endPage == deadpage && tail == 0)) {
                deadpages.add(endPage);
                done = true;
            } else if (endPage == deadpage) {
                // there is good data on the "endPage" (past the end of the 
                // old object):  1) copy it off; 2) fill "page" (past teh 
                // end of the new object); 3) reuse endPage by copying any 
                // of the good data that did not fit on the end of "page" 
                assert tail > 0;
                byte[] gooddata = new byte[tail];
                endPage.read(gooddata, 0, endPageOffset, tail);
                int iobytes = 0;
                if (page.limit() < page.size()) {
                    iobytes = page.write(gooddata, 0, page.limit(), tail);
                    pageOffset += iobytes;
                    page.setLimit(page.limit() + iobytes);
                    assert page.limit() == pageOffset;
                }

                if (iobytes < tail) {
                    assert page != endPage;
                    iobytes = endPage.write(gooddata, iobytes, 0, tail
                            - iobytes);
                    endPage.setLimit(iobytes);
                } else {
                    deadpages.add(deadpage);
                }
                done = true;
            } else {
                deadpages.add(deadpage);
                deadindex++;
            }
        }

        pages.removeAll(deadpages);
        for (PageDescriptor deadpage : deadpages) {
            pageManager.releasePageDescriptor(deadpage);
        }
    }

    void insert() {
        assert endPage == page && pageOffset == endPageOffset;
        byte[] leftover = null;

        if (endPage.limit() - endPageOffset > 0) {
            // there's good data past the end of the old object, so we copy it
            // into leftover
            leftover = new byte[endPage.limit() - endPageOffset];
            endPage.read(leftover, 0, endPageOffset, endPage.limit()
                    - endPageOffset);
            endPage.setLimit(endPageOffset);
        }
        index++;
        List<PageDescriptor> newPages = new LinkedList<PageDescriptor>();
        int iobytes = 0;
        // write the rest of the new object onto new pages
        while (total < length) {
            assert pageOffset == page.limit();
            iobytes = page.write(buffer, bufferOffset, pageOffset, remaining);
            advance(iobytes);

            if (pageOffset > page.limit()) {
                page.setLimit(pageOffset);
            }
            
            if(log.isTraceEnabled()) {
                log.trace("INSERT total "+total+" leng "+length+" index "+index 
                        +" size of pages "+pages.size()+" remaining"+remaining 
                        +" pageOffset " + pageOffset+" page limit " + page.limit());
            }
            
            if (total < length) {
                pageOffset = 0;
                page = pageManager.pageDescriptor(id);
                newPages.add(page);
            }
        }
        // take care of the leftover data.  first try to copy it to the end of
        // the current page.  if it did not fit then we create a new page and 
        // write the remains of leftover.  next we insert all the new pages.
        iobytes = 0;
        if (leftover != null && page.limit() < page.size()) {
            iobytes = page.write(leftover, 0, page.limit(), leftover.length);
            page.setLimit(page.limit() + iobytes);
        }

        if (leftover != null && iobytes < leftover.length) {
            page = pageManager.pageDescriptor(id);
            newPages.add(page);
            iobytes = page.write(leftover, iobytes, 0, leftover.length
                    - iobytes);
            page.setLimit(iobytes);
        }
        pages.addAll(index, newPages);
    }
}
