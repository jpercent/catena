package syndeticlogic.catena.store;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.memento.Pinnable;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.ThreadSafe;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

@ThreadSafe
public class Segment implements Pinnable {

    private static final Log log = LogFactory.getLog(Segment.class);

    private String fileName;
    private PageManager pageManager;
    //private SegmentHeader header;
    private SerializedObjectChannel channel;
    private ReadWriteLock rwlock;
    private List<Page> pageVector;
    private HashMap<Integer, Long> pageOffsets;
    private int pinCount;
    private long size;
    private Type type;

    public Segment() {
    }

    public Segment(Type type, SegmentHeader header, SerializedObjectChannel channel, ReadWriteLock lock,
            PageManager pagemanager, String filename) {

        rwlock = lock;
        pageManager = pagemanager;
        fileName = filename;
        pageManager.createPageSequence(fileName);
        pageVector = pageManager.getPageSequence(fileName);
        //this.header = header;
        header.type(type);
        header.store();
        SegmentReader reader = new SegmentReader(header, channel, pageManager,pageVector, fileName);
        pageOffsets = reader.load();
        size = 0;
        pinCount = 0;
        this.channel = channel;
        if (log.isTraceEnabled())
            log.trace("completed construction");
    }

    public Segment(ReadWriteLock lock, SegmentHeader header, SerializedObjectChannel channel,
            PageManager pagemanager, String filename) {
        this.rwlock = lock;
        this.pageManager = pagemanager;
        this.fileName = filename;
        pageVector = pageManager.getPageSequence(fileName);
        this.channel = channel;
        //this.header = header;
        header.load();
        this.type = header.type();
        this.size = header.dataSize();
        SegmentReader reader = new SegmentReader(header, channel, pageManager,pageVector, fileName);
        pageOffsets = reader.load();
        pinCount = 0;
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
    
    public int scan(byte[] buffer, int bufferOffset, long fileOffset, int length) {
        int total = 0;
        rwlock.readLock().lock();
        try {
            PageScan pageIO = new PageScan(pageManager, fileName);
            total = pageIO.scan(buffer, bufferOffset, length, fileOffset);
        } finally {
            rwlock.readLock().unlock();
        }
        return total;
    }
    
    public void update(byte[] buffer, int bufferOffset, long fileOffset,
            int oldLen, int newLen) {
        rwlock.writeLock().lock();
        try {
            PageUpdate pageIO = new PageUpdate(pageManager, fileName);
            pageIO.update(buffer, bufferOffset, oldLen, newLen,
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
            PageAppend pageIO = new PageAppend(pageManager, fileName);
            pageIO.append(buffer, bufferOffset, size);
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
            // note that the update handles setting the value of size
        } finally {
            rwlock.writeLock().unlock();
        }
    }
    
    public void commit() {
        rwlock.readLock().lock();
        try {
            SegmentWriter segmentWriter = new SegmentWriter(channel, pageManager, pageOffsets, fileName);
            segmentWriter.write();
        } finally {
            rwlock.readLock().unlock();
        }
    }

    public long size() {
        long size = 0;
        rwlock.readLock().lock();
        try {
            size = this.size;
        } finally {
            rwlock.readLock().unlock();
        }
        return size;
    }
    
    public Segment split(long offset) {
        rwlock.writeLock().lock();
        // do something
        rwlock.writeLock().unlock();
        return null;
    }
    
    public synchronized Type getType() {
        return type;
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
}
