package syndeticlogic.catena.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.utility.Observer;
import syndeticlogic.catena.utility.ThreadSafe;

import syndeticlogic.catena.store.Page.PageState;

import java.nio.ByteBuffer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@ThreadSafe
public class PageManager {

    private static final Log log = LogFactory.getLog(PageManager.class);
    private final PageFactory factory;
    private final Observer observer;
    private final HashMap<String, List<Page>> pageSequences;
    private final ConcurrentLinkedQueue<ByteBuffer> freelist;
    private final ReentrantLock lock;
    private final Condition condition;
    private final int pageSize;
    private final int retryLimit;

    public PageManager() {
        factory = null;
        observer = null;
        pageSequences = null;
        freelist = null;
        lock = null;
        condition = null;
        pageSize = -1;
        retryLimit = 2;
    }

    public PageManager(PageFactory factory, Observer observer, HashMap<String, 
            List<Page>> pageSequences, ConcurrentLinkedQueue<ByteBuffer> freelist, 
            int pageSize, int retryLimit) {
        this.factory = factory;
        this.observer = observer;
        this.freelist = freelist;
        this.pageSequences = pageSequences;
        this.pageSize = pageSize;
        lock = new ReentrantLock();
        condition = lock.newCondition();
        this.retryLimit = retryLimit;
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
                observer.notify(page);
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
        ByteBuffer buffer = byteBuffer();
        page.attachBuffer(buffer);
        page.pin();
        page.setLimit(0);
        page.setDataSegmentId(identifier);
        page.state(PageState.FREE);
        observer.notify(page);
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

    public ByteBuffer byteBuffer() {
        ByteBuffer buf=null;
        try {
            lock.lock();
            buf = getBuf();
        } finally {
            lock.unlock();
        }
        return buf;
    }

    public void releaseByteBuffer(ByteBuffer buf) {
        try {
            lock.lock();
            freelist.add(buf);
        } finally {
            lock.unlock();
        }
    }

    public void free(Page page) {
        try {
            lock.lock();
            ByteBuffer buffer = page.detachBuffer();
            freelist.add(buffer);
            page = null;
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public int pageSize() {
        return pageSize;
    }

    // must hold the page lock
    private ByteBuffer getBuf() {
        // we wait for some time and then presume deadlock has occured and throw an exception. c
        // deadlock resolution is handled by the upper layers
        
        ByteBuffer buffer = null;
        if (freelist.size() == 0)
            observer.notify(null);
        
        int retries = 0;            
        while (true) {
            try {
                buffer = freelist.poll();
                if (buffer == null) {
                    long timelimit = 8L * 1000L * 1000L * 1000L;
                    long then = System.nanoTime();

                    while(timelimit > 0) {
                        condition.awaitNanos(timelimit);
                        timelimit -= (System.nanoTime() - then);
                    }

                    retries ++;
                    if(retries < retryLimit) {
                        continue;
                    } else {
                        throw new RuntimeException("pageManager waiting for page, retries exceeded");
                    }
                } 
                break;
            } catch (InterruptedException e) {
                log.info("pageManager interrupted while waiting for pages; retrying", e);
            }
        }
       
        assert buffer != null;
        return buffer;
    }

    // must hold the page lock
    // pages can be released via releasePageDescriptor and releasePageSequence.  releasePageDescriptor 
    // could be called by releasePageSequence and such factor down the overhead of calling 
    // releasePage, but to do so would result in retaking the reentrant lock, which we think is more
    // expensive than a method call. 
    private void releasePage(Page page) {
        assert page != null;
        page.unpin();
        observer.notify(page);
    }
}
