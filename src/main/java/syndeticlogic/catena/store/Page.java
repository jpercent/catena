package syndeticlogic.catena.store;

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import syndeticlogic.catena.utility.Observeable;

public class Page implements Observeable {
    public enum PageState implements State { FREE, PINNED, UNPINNED }

    private static final Log log = LogFactory.getLog(Page.class);
    private ByteBuffer buffer;
    private String segmentId;
    private int effectiveSize;
    private int limit;
    private boolean dirty;
    private int pinCount;
    private PageState state;
    
    public Page() {
        this.segmentId = "";
        this.buffer = null;
        this.dirty = false;
        this.pinCount = 0;
        this.effectiveSize = -1;
        this.state = PageState.FREE;
    }
 
    @Override
    public void state(State state) {
        assert state instanceof PageState;
        this.state = (PageState)state;
    }
    
    @Override
    public State state() {
        return this.state;
    }
    
    public int read(byte[] dest, int destOffset, int pageOffset, int size) {
        assert size > 0;
        ByteBuffer copy = buffer.asReadOnlyBuffer();
        copy.position(pageOffset);
        if (size + pageOffset > copy.limit()) {
            size = copy.limit() - copy.position();
        }
        if (log.isTraceEnabled())
            log.debug("dest length " + dest.length + " dest offset "
                    + destOffset + " size " + size + " position "
                    + copy.position());
        assert dest.length >= destOffset + size;
        // byte[] buf = buffer.array();
        copy.get(dest, destOffset, size);
        return size;
    }

    public int read(ByteBuffer dest, int pageOffset) {
        int size = dest.limit() - dest.position();
        assert dest.limit() > 0;

        ByteBuffer copy = buffer.asReadOnlyBuffer();
        copy.position(pageOffset);
        int limit = dest.limit();
        if (size + pageOffset > copy.limit()) {
            size = copy.limit() - copy.position();
            dest.limit(dest.position() + size);
        } else {
            copy.limit(pageOffset + size);
        }

        if (log.isTraceEnabled())
            log.debug("dest length " + dest.limit() + " dest offset "
                    + dest.position() + " size " + size + " position "
                    + copy.position() + " copy.limit" + copy.limit());

        assert dest.capacity() >= dest.position() + size;
        dest.put(copy);
        dest.limit(limit);
        return size;
    }

    public int write(byte[] source, int sourceOffset, int pageOffset, int size) {
        buffer.position(pageOffset);
        if (size > buffer.limit() - buffer.position()) {
            size = buffer.limit() - buffer.position();
        }

        if (log.isTraceEnabled()) {
            log.trace("pageOffset " + pageOffset + " src offset "
                    + sourceOffset + " srclen " + source.length + " size "
                    + size);
        }

        buffer.put(source, sourceOffset, size);
        buffer.clear();
        dirty = true;
        return size;
    }

    public int write(ByteBuffer source, int pageOffset) {
        buffer.position(pageOffset);
        int size = source.limit() - source.position();
        int limit = 0;
        if (size > buffer.limit() - buffer.position()) {
            size = buffer.limit() - buffer.position();
            limit = source.limit();
            source.limit(source.position() + size);
        }
        buffer.put(source);
        buffer.clear();
        dirty = true;
        source.limit(limit);
        return size;
    }

    public int attachBuffer(ByteBuffer buf) {
        assert this.buffer == null;
        buffer = buf;
        effectiveSize = buffer.limit();
        limit = effectiveSize;
        return effectiveSize;
    }

    public boolean isDirect() {
        boolean isdirect = false;
        if (buffer != null)
            isdirect = buffer.isDirect();
        return isdirect;
    }

    public ByteBuffer detachBuffer() {
        assert pinCount == 0 && buffer != null;
        ByteBuffer buf = buffer;
        dirty = false;
        effectiveSize = -1;
        limit = effectiveSize;
        return buf;
    }

    public void pin() {
        assert buffer != null;
        pinCount++;
    }

    public void unpin() {
        assert pinCount > 0 && buffer != null;
        pinCount--;
    }

    public int size() {
        return effectiveSize;
    }

    public String getDataSegmentId() {
        return segmentId;
    }

    public void setDataSegmentId(String fileName) {
        this.segmentId = fileName;
    }

    public boolean isAssigned() {
        boolean assigned = (this.buffer != null ? true : false);
        return assigned;
    }

    public boolean isPinned() {
        assert pinCount >= 0;
        return (pinCount != 0);
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setLimit(int limit) {
        assert limit <= effectiveSize;
        this.limit = limit;
    }

    public int limit() {
        int ret = -1;
        ret = limit;
        return ret;
    }

    public ByteBuffer getBuffer() {
        return this.buffer;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + super.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;

        if (getClass() != obj.getClass())
            return false;
        
        if (this == obj)
            return true;
        else 
            return false;
    }
}