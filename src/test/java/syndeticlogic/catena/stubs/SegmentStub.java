package syndeticlogic.catena.stubs;


import syndeticlogic.catena.store.Segment;
import syndeticlogic.catena.type.Value;
import syndeticlogic.catena.type.Type;

public class SegmentStub extends Segment {
    public int size = 0;
    public byte[] lastBuf = null;
    public int lastSize = 0;
    public int lastBufOffset = 0;
    public long lastSegmentOffset = 0;
    public int lastLength = 0;
    public String name;
    public boolean split = false;
    public long splitOffset = 0;
    public boolean readLock = false;
    public boolean writeLock = false;
    public Type type=null;
    public long deleteOffset = 0;
    public int deleteLength = 0;
    public boolean pin = false;
    
    @Override
    public void unpin() {
        pin = false;
    }

    @Override
    public void pin() {
        pin = true;
    }

    @Override
    public boolean isPinned() {
        return false;
    }

    @Override
    public void acquireReadLock() {
        readLock = true;
    }

    @Override
    public void acquireWriteLock() {
        writeLock = true;
    }

    @Override
    public void releaseReadLock() {
        readLock = false;
    }

    @Override
    public void releaseWriteLock() {
        writeLock = false;
    }

    @Override
    public String getQualifiedFileName() {
        return name;
    }
/*
    @Override
    public Type getType() {
        return type;
    }

    @Override
    public void setType(Type type) {
        this.type = type;
    }
*/

    @Override
    public long size() {
        return size;
    }

    @Override
    public int scan(byte[] buffer, int bufferOffset, long segmentOffset, int length) {
        lastBuf = buffer;
        lastBufOffset = bufferOffset;
        lastSegmentOffset = segmentOffset;
        lastLength = length;
        return length;
    }

    @Override
    public void update(byte[] buffer, int bufferOffset, long fileOffset, int oldLength, int newLength) {
    }

    @Override
    public void append(byte[] buffer, int offset, int length) {
        lastBuf = buffer;
        lastBufOffset = offset;
        lastLength = length;
        size += length;
    }

    @Override
    public void delete(long offset, int length) {
        deleteOffset = offset;
        deleteLength = length;
    }

    @Override
    public Segment split(long offset) {
        split = true;
        splitOffset = offset;
        return null;
    }

    @Override
    public void commit() {
        
    }

}
