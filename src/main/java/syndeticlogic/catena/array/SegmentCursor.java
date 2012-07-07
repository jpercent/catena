package syndeticlogic.catena.array;

import syndeticlogic.catena.store.Segment;

import syndeticlogic.catena.array.Array.LockType;

public class SegmentCursor {
    private Segment segment;
    private int offset;
    private LockType lockType;

    public void configure(Segment segment, int offset, LockType lockType) {
        this.segment = segment;
        this.offset = offset;
        this.lockType = lockType;
    }
    
    public void reconfigure(Segment segment, int offset) {
        assert lockType != null;
        this.segment = segment;
        this.offset = offset;
    }
        
    public int scan(byte[] buffer, int boffset, int size) {
        assert segment != null && offset >= 0 && offset < segment.size();
        int scanned = segment.scan(buffer, boffset, offset, size);
        offset += scanned;
        assert offset <= segment.size();
        return scanned;
    }
    
    public void update(byte[] buffer, int boffset, int oldSize, int newSize) {
        segment.update(buffer, boffset, offset, oldSize, newSize);
    }
    
    public void append(byte[] buffer, int boffset, int size) {
        assert segment != null;
        segment.append(buffer, boffset, size);
    }
    
    public void delete(int size) {
        segment.delete(offset, size);
    }
    
    public boolean scanned() {
        if(segment == null || offset < 0) {
            return false;
        } else {
            assert segment != null && offset >= 0 && offset <= segment.size();
            return (offset == segment.size() ? true : false);
        }
    }
    
    public int remaining() {
        if(offset == -1 || segment == null) {
            return -1;
        }
        return segment.size() - offset;
    }
    
    public Segment segment() {
        return segment;
    }
    
    public int offset() {
        return offset;
    }
    
    public LockType lockType() {
        return lockType;
    }
}
