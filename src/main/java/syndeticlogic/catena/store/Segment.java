package syndeticlogic.catena.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.memento.Pinnable;
import syndeticlogic.catena.codec.CodeHelper;
import syndeticlogic.catena.codec.Codec;
import syndeticlogic.catena.type.Value;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.ThreadSafe;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReadWriteLock;

@ThreadSafe
public class Segment implements Pinnable {

    private static final Log log = LogFactory.getLog(Segment.class);

    private String fileName;
    private Value largest;
    private Value smallest;
    private PageManager pageManager;
    private PersistentIOHandler ioHandler;
    private ReadWriteLock rwlock;
    private List<PageDescriptor> pageVector;
    private int pinCount;
    private int size;

    public Segment() {
    }
    
    public Segment(Type type, FileChannel channel,
            ReadWriteLock lock, PageManager pagemanager, ExecutorService pool,
            String filename) {
        try {
            rwlock = lock;
            pageManager = pagemanager;
            fileName = filename;
            pageManager.createPageSequence(fileName);
            pageVector = pageManager.getPageSequence(fileName);
            size = 0;

            ioHandler = new PersistentIOHandler(type, channel, pageManager,
                    pageVector, pool, fileName);
            ioHandler.writeHeader(size);
            ioHandler.load();
            pinCount = 0;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        if (log.isTraceEnabled())
            log.trace("completed construction");
    }

    public Segment(ReadWriteLock lock, FileChannel channel, PageManager pagemanager,
            ExecutorService pool, String filename) 
    {
        try {
            this.rwlock = lock;
            this.pageManager = pagemanager;
            this.fileName = filename;

            pageVector = pageManager.getPageSequence(fileName);

            CodeHelper coder = Codec.getCodec().coder();
            ByteBuffer buff = pageManager.byteBuffer();
            ioHandler = new PersistentIOHandler(null, channel, pageManager,
                    pageVector, pool, fileName);
            size = ioHandler.unpackHeader(coder, buff);
            buff.rewind();
            buff.limit(buff.capacity());
            pinCount = 0;
            ioHandler.load();
            pageManager.releaseByteBuffer(buff);

        } catch (FileNotFoundException e) {
            log.error("file "+filename+" not found exception ", e); 
            throw new RuntimeException(e);
        } catch (IOException e) {
            log.error("I/O exception ", e); 
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            log.error("Interrupted exception ", e); 
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            log.error("Execution exception ", e); 
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void unpin() {
        pinCount--;
        assert pinCount >= 0;
        if (pinCount == 0) {
            pageManager.releasePageSequence(pageVector);
        }
    }

    @Override
    public synchronized void pin() {
        assert pinCount >= 0;
        if(pinCount == 0) {
            pageVector = pageManager.getPageSequence(fileName);
        }
        pinCount++;
    }

    @Override
    public synchronized boolean isPinned() {
        assert pinCount >= 0;
        return (pinCount > 0 ? true : false);
    }
    
    public void acquireReadLock() {
        rwlock.readLock().lock();
    }
    
    public void releaseReadLock() {
        rwlock.readLock().unlock();
    }
    
    public void acquireWriteLock() {
        rwlock.writeLock().lock();
    }
    
    public void releaseWriteLock() {
        rwlock.writeLock().unlock();
    }
    
    public String getQualifiedFileName() {
        return fileName;
    }
    
    public synchronized Type getType() {
        return ioHandler.type();
    }
    
    public synchronized void setType(Type type) {
        throw new RuntimeException("not supported");
        // this.type = type;
    }

    public int size() {
        int size = 0;
        rwlock.readLock().lock();
        try {
            size = this.size;
        } finally {
            rwlock.readLock().unlock();
        }
        return size;
    }

    
    public synchronized Value getSmallest() {
        return smallest;
    }

    
    public synchronized void setSmallest(Value smallest) {
        this.smallest = smallest;
    }

    
    public synchronized Value getLargest() {
        return largest;
    }

    
    public synchronized void setLargest(Value largest) {
        this.largest = largest;
    }
    
    public Segment split(long offset) {
        rwlock.writeLock().lock();
        // do something
        rwlock.writeLock().unlock();
        return null;
    }

    
    public int scan(byte[] buffer, int bufferOffset, long fileOffset, int length) {
        int total = 0;
        rwlock.readLock().lock();
        try {
            PageIOHandler pageIOHandler = new PageIOHandler(pageManager, fileName);
            total = pageIOHandler.scan(buffer, bufferOffset, length, fileOffset);
        } finally {
            rwlock.readLock().unlock();
        }
        return total;
    }

    
    public void update(byte[] buffer, int bufferOffset, long fileOffset,
            int oldLen, int newLen) {
        rwlock.writeLock().lock();
        try {
            PageIOHandler pageIOHandler = new PageIOHandler(pageManager, fileName);
            pageIOHandler.update(buffer, bufferOffset, oldLen, newLen,
                    fileOffset);
            size -= oldLen;
            size += newLen;
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    
    public void append(byte[] buffer, int bufferOffset, int size) {
        rwlock.writeLock().lock();
        try {
            PageIOHandler pageIOHandler = new PageIOHandler(pageManager, fileName);
            pageIOHandler.append(buffer, bufferOffset, size);
            this.size += size;
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    
    public void delete(long start, int length) {
        assert length > 0;
        byte[] single = new byte[1];
        rwlock.writeLock().lock();
        try {
            if (start > 0) {
                scan(single, 0, start - 1, 1);
                update(single, 0, start - 1, length + 1, 1);
            } else {
                assert start == 0;
                scan(single, 0, length, 1);
                update(single, 0, start, length + 1, 1);
            }
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    
    public void commit() {
        rwlock.readLock().lock();
        try {
            ioHandler.commit(size);
        } catch (IOException e) {
            log.error("IOException", e);
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            log.error("InterruptedException", e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            log.error("ExecutionException", e);
            throw new RuntimeException(e);
        } finally {
            rwlock.readLock().unlock();
        }
    }
}
