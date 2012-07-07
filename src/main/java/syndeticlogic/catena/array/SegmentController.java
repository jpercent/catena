package syndeticlogic.catena.array;

import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import syndeticlogic.catena.store.Segment;

import syndeticlogic.catena.array.Array.LockType;

public class SegmentController {

    private static final Log log = LogFactory.getLog(SegmentController.class);
    private ArrayDescriptor arrayDescriptor;

    public SegmentController(ArrayDescriptor arrayDesc) {
        this.arrayDescriptor = arrayDesc;
    }

    public void findAndLockSegment(SegmentCursor segmentCursor, LockType lockType, long offset) {
        assert segmentCursor != null && lockType != null && offset >= 0;
        arrayDescriptor.acquire();
        try {
            
            Collection<Segment> segments = arrayDescriptor.segments();
            assert segments != null;
            Iterator<Segment> iterator = segments.iterator();

            Segment segment = null;
            long current = 0;
            while (iterator.hasNext()) {
                segment = iterator.next();
                assert segment != null;
                if (current + segment.size() > offset)
                    break;
                current += segment.size();
            }
            assert segment != null;
            if (current + segment.size() < offset) {
                return;
            }

            assert offset - current < Integer.MAX_VALUE;
            int localOffset = (int) (offset - current);
            lockSegment(segment, lockType);
            segmentCursor.configure(segment, localOffset, lockType);

        } finally {
            arrayDescriptor.release();
        }
    }

    public boolean unlockAndLockNextSegment(SegmentCursor segmentCursor) {
        boolean found = false;
        arrayDescriptor.acquire();
        try {
            Collection<Segment> segments = arrayDescriptor.segments();
            boolean next = false;

            for (Segment segment : segments) {
                if (next) {
                    lockSegment(segment, segmentCursor.lockType());
                    unlockSegment(segmentCursor.segment(), segmentCursor.lockType());
                    segmentCursor.reconfigure(segment, 0);
                    found = true;
                    break;
                }

                if (segment == segmentCursor.segment()) {
                    next = true;
                }
            }
        } finally {
            arrayDescriptor.release();
        }
        
        if(!found) {
            unlockSegment(segmentCursor.segment(), segmentCursor.lockType());
            segmentCursor.configure(null, -1, null);
        }
        
        return found;
    }

    public void unlockSegment(SegmentCursor segmentCursor, LockType lockType) {
        unlockSegment(segmentCursor.segment(), lockType);
        segmentCursor.configure(null, 0, null);
    }

    private void lockSegment(Segment segment, LockType lockType) {
        switch (lockType) {
        case ReadLock:
            if(log.isTraceEnabled()) 
                log.trace(arrayDescriptor.id() + "blocking on write-lock for segment ");
            segment.acquireReadLock();
            break;
        case WriteLock:
            if(log.isTraceEnabled()) 
                log.trace(arrayDescriptor.id() + "blocking on write-lock for segment ");
            segment.acquireWriteLock();
            break;
        }
    }

    private void unlockSegment(Segment segment, LockType lockType) {
        switch (lockType) {
        case ReadLock:
            if(log.isTraceEnabled()) 
                log.trace(arrayDescriptor.id() + "releasing read-lock on segment ");
            segment.releaseReadLock();
            break;
        case WriteLock:
            if(log.isTraceEnabled()) 
                log.trace(arrayDescriptor.id() + "releaseing write-lock on segment ");
            segment.releaseWriteLock();
            break;
        }
    }
}
