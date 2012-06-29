package syndeticlogic.catena.store;

import syndeticlogic.memento.Cache;
import syndeticlogic.memento.EvictionListener;
import syndeticlogic.memento.PinnableListener;
import syndeticlogic.catena.utility.Observeable;
import syndeticlogic.catena.utility.Observer;
import syndeticlogic.catena.utility.ThreadSafe;

import syndeticlogic.catena.store.PageDescriptor.PageState;

@ThreadSafe
public class PageCache implements Observer, EvictionListener {
    private final PageManager pageManager;
    private final Cache cache;
    private final PinnableListener pinnableListener;
    
    public PageCache(PageManager pageManager, Cache cache, PinnableListener pinnableListener) {
        this.pageManager = pageManager;
        this.cache = cache;
        this.pinnableListener = pinnableListener;
        assert this.pageManager != null && this.cache != null && this.pinnableListener != null;
    }
    
     public void notify(Observeable observeable) {     
        if(observeable == null) {
            // XXX - this is a hack; find a better way
            cache.removeLeastValuableNode();
        } else {
            PageDescriptor page = (PageDescriptor)observeable;
            PageState pstate = (PageState)page.state();
            if(pstate == PageState.FREE) {
                cache.addUncachedObject(page, page);
                page.state(PageState.PINNED);
            } else if(pstate == PageState.PINNED){                
                if (!page.isPinned()) {
                    if(pinnableListener != null) {
                        pinnableListener.unpin(page);
                    }
                }
                pstate = PageState.UNPINNED;
            } else if(pstate == PageState.UNPINNED) {
                assert page.isPinned() && page.isAssigned();
                cache.addObject(page,page);
                pstate = PageState.PINNED;
            } else {
                throw new RuntimeException("Unsupported page state - check module version alignment");
            }
        }
    }

     @Override
    public void evicted(Object userKey, Object cacheObject) {
        assert cacheObject instanceof PageDescriptor;
        pageManager.free((PageDescriptor)cacheObject);
        userKey = null;
        cacheObject = null;
    }
}
