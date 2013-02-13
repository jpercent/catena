package syndeticlogic.catena.utility;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BufferPool<T> {
	private static final Log log = LogFactory.getLog(BufferPool.class);
	private final ConcurrentLinkedQueue<T> freelist;
	private final ReentrantLock lock;
    private final Observer observer;
	private final Condition condition;
	private final int retryLimit;
	
	public BufferPool() {
        freelist = null;
        lock = null;
        observer = null;
        condition = null;
        retryLimit = 2;
	}
	
    public BufferPool(ConcurrentLinkedQueue<T> freelist, Observer observer, int retryLimit) {       
        this.freelist = freelist;
        this.observer = observer;
        lock = new ReentrantLock();
        condition = lock.newCondition();
        this.retryLimit = retryLimit;
    }

    public T buffer() {
        T buffer=null;
        try {
            lock.lock();
            // we wait for some time and then presume deadlock has occured and throw an exception. c
            // deadlock resolution is handled by the upper layers      
            if (freelist.size() == 0) {
            	// evict a buffer from the cache
                observer.notify(null);
            }

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
        } finally {
            lock.unlock();
        }
    }

    public void release(T buffer) {
        try {
            lock.lock();
            freelist.add(buffer);
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
