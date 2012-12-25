package syndeticlogic.catena.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.predicate.Predicate;

public class PageIOAppend extends PageIOState {
    protected static final Log log = LogFactory.getLog(PageIOState.class);
    
    PageIOAppend(Predicate predicate, PageManager pageManager, String id) {
        super(predicate, pageManager, id);
    }

    public void append(byte[] buf, int bufOffset, int size) {
        prepareAppend(buf, bufOffset, size);

        if (page.size() == page.limit()) {
            Page newPage = pageManager.page(id);
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
                page = pageManager.page(id);
                pages.add(page);
            }
        }
        complete();
    }
}
