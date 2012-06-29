package syndeticlogic.catena.array;

import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import syndeticlogic.catena.store.Segment;

import syndeticlogic.catena.array.Array.LockType;

public class SegmentController {

    private static final Log log = LogFactory.getLog(SegmentController.class);
    private ArrayDescriptor arrayDesc;

    public SegmentController(ArrayDescriptor arrayDesc) {
        this.arrayDesc = arrayDesc;
    }

    public void findAndLockSegment(SegmentCursor sc, LockType lt, long offset) {
        assert sc != null && lt != null && offset >= 0;
        arrayDesc.acquire();
        try {
            
            Collection<Segment> segments = arrayDesc.segments();
            assert segments != null;
            Iterator<Segment> iterator = segments.iterator();

            Segment s = null;
            long current = 0;
            while (iterator.hasNext()) {
                s = iterator.next();
                assert s != null;
                if (current + s.size() > offset)
                    break;
                current += s.size();
            }
            assert s != null;
            if (current + s.size() < offset) {
                return;
            }

            assert offset - current < Integer.MAX_VALUE;
            int localOffset = (int) (offset - current);
            lockSegment(s, lt);
            sc.configure(s, localOffset, lt);

        } finally {
            arrayDesc.release();
        }
    }

    public boolean unlockAndLockNextSegment(SegmentCursor sc) {
        boolean found = false;
        arrayDesc.acquire();
        try {
            Collection<Segment> segments = arrayDesc.segments();
            boolean next = false;

            for (Segment seg : segments) {
                if (next) {
                    lockSegment(seg, sc.lockType());
                    unlockSegment(sc.segment(), sc.lockType());
                    sc.reconfigure(seg, 0);
                    found = true;
                    break;
                }

                if (seg == sc.segment()) {
                    next = true;
                }
            }
        } finally {
            arrayDesc.release();
        }
        
        if(!found) {
            unlockSegment(sc.segment(), sc.lockType());
            sc.configure(null, -1, null);
        }
        
        return found;
    }

    public void unlockSegment(SegmentCursor sc, LockType lt) {
        unlockSegment(sc.segment(), lt);
        sc.configure(null, 0, null);
    }

    private void lockSegment(Segment s, LockType lt) {
        switch (lt) {
        case ReadLock:
            if(log.isTraceEnabled()) 
                log.trace(arrayDesc.id() + "blocking on write-lock for segment ");
            s.acquireReadLock();
            break;
        case WriteLock:
            if(log.isTraceEnabled()) 
                log.trace(arrayDesc.id() + "blocking on write-lock for segment ");
            s.acquireWriteLock();
            break;
        }
    }

    private void unlockSegment(Segment s, LockType lt) {
        switch (lt) {
        case ReadLock:
            if(log.isTraceEnabled()) 
                log.trace(arrayDesc.id() + "releasing read-lock on segment ");
            s.releaseReadLock();
            break;
        case WriteLock:
            if(log.isTraceEnabled()) 
                log.trace(arrayDesc.id() + "releaseing write-lock on segment ");
            s.releaseWriteLock();
            break;
        }
    }
}
