package syndeticlogic.catena.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.predicate.Predicate;

public class PageIOScan extends PageIOState {
    protected static final Log log = LogFactory.getLog(PageIOScan.class);
 
    public PageIOScan(Predicate predicate, PageManager pageManager, String id) {
        super(predicate, pageManager, id);
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
    
    protected void prepareScan(byte[] buffer, int bufferOffset, int length,
            long loffset) {
        prepare(buffer, bufferOffset, length, loffset);
        setIndexAndPageOffset();
    }
}
