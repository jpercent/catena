package syndeticlogic.catena.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.utility.BufferPool;
import syndeticlogic.catena.utility.Observer;
import syndeticlogic.catena.utility.ThreadSafe;

import syndeticlogic.catena.store.Page.PageState;

import java.nio.ByteBuffer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

@ThreadSafe
public class PageManager {

    private static final Log log = LogFactory.getLog(PageManager.class);
    private final PageFactory factory;
    private final Observer pageCacheObserver;
    private final HashMap<String, List<Page>> pageSequences;
    private final BufferPool<ByteBuffer> bufferPool;
    private final ReentrantLock lock;
    private final int pageSize;

    public PageManager() {
        factory = null;
        pageCacheObserver = null;
        pageSequences = null;
        bufferPool = null;
        lock = null;
        pageSize = -1;
    }

    public PageManager(PageFactory factory, Observer observer, HashMap<String, 
            List<Page>> pageSequences, BufferPool<ByteBuffer> bufferPool, ConcurrentLinkedQueue<ByteBuffer> freelist, 
            int pageSize, int retryLimit) {
        this.factory = factory;
        this.pageCacheObserver = observer;
        this.pageSequences = pageSequences;
        this.bufferPool = bufferPool;
        this.pageSize = pageSize;
        lock = new ReentrantLock();
    }

    public void createPageSequence(String key) {
        try {
            lock.lock();
            if (pageSequences.containsKey(key)) {
                log.error("throw runtime exception because key already exits");
                throw new RuntimeException("key already exists");
            }
            pageSequences.put(key, new LinkedList<Page>());
        } finally {
            lock.unlock();
        }
    }

    public List<Page> getPageSequence(String key) {
        List<Page> pageVector = null;
        try {
            lock.lock();
            assert pageSequences != null;
            pageVector = pageSequences.get(key);
            for(Page page : pageVector) {
                page.pin();
                pageCacheObserver.notify(page);
            }
        } finally {
            lock.unlock();
        }
        return (List<Page>) pageVector;
    }

    public void releasePageSequence(List<Page> pageVector) {
        try {
            lock.lock();
            for (Page page : pageVector)
                releasePage(page);
        } finally {
            lock.unlock();
        }
    }

    public Page page(String identifier) {
        Page page = factory.createPageDescriptor();
        ByteBuffer buffer = bufferPool.buffer();
        page.attachBuffer(buffer);
        page.pin();
        page.setLimit(0);
        page.setDataSegmentId(identifier);
        page.state(PageState.FREE);
        pageCacheObserver.notify(page);
        return page;
    }

    public void releasePageDescriptor(Page pagedes) {
        try {
            lock.lock();
            releasePage(pagedes);
        } finally {
            lock.unlock();
        }
    }

    // must hold the page lock
    // pages can be released via releasePageDescriptor and releasePageSequence.  releasePageDescriptor 
    // could be called by releasePageSequence and such factor down the overhead of calling 
    // releasePage, but to do so would result in retaking the reentrant lock, which we think is more
    // expensive than a method call. 
    private void releasePage(Page page) {
        assert page != null;
        page.unpin();
        pageCacheObserver.notify(page);
    }

    public void free(Page page) {
        try {
            lock.lock();
            ByteBuffer buffer = page.detachBuffer();
            bufferPool.release(buffer);
            page = null;
        } finally {
            lock.unlock();
        }
    }
    
    public int pageSize() {
        return pageSize;
    } 
}
