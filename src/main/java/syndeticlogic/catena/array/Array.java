/*
 *   Copyright 2010 - 2012 James Percent
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package syndeticlogic.catena.array;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.predicate.Predicate;
import syndeticlogic.catena.store.Segment;
import syndeticlogic.catena.utility.Transaction;
import syndeticlogic.catena.array.SegmentCursor;

/**
 * @author <a href="mailto:james@empty-set.net">James Percent</a>
 */
public class Array {

    public enum LockType { ReadLock, WriteLock };  
    private static final Log log = LogFactory.getLog(Array.class);
    private ArrayDescriptor arrayDescriptor;
    private SegmentController segmentController;
    private SegmentCursor segmentCursor;
    private int index;
    private boolean configured;
    
    Array(ArrayDescriptor descriptor, SegmentController segmentController) {
        assert descriptor != null;
        this.arrayDescriptor = descriptor;
        this.segmentController = segmentController;
        this.index = 0;
        this.segmentCursor = new SegmentCursor();
    }

    public IODescriptor scan(IODescriptor ioDescriptor) {
        if (!configured) { return null; }
        if (log.isTraceEnabled()) { log.debug("pos=" + index); }
        
        int size = ioDescriptor.ioSize();
        int offset = ioDescriptor.offset();
        int accumulation = 0;
        boolean moreToScan = true;

        while (accumulation < size && moreToScan) {
            int scanSize = ioDescriptor.recordValuesScanned(segmentCursor);
            int scanned = segmentCursor.scan(ioDescriptor.buffer(), offset, scanSize);
            assert scanSize == scanned;
            accumulation += scanned;
            offset += scanned;

            if (accumulation < size) {
                moreToScan = segmentController.unlockAndLockNextSegment(segmentCursor);
            }
        }

        if (moreToScan && segmentCursor.scanned()) {
            moreToScan = segmentController.unlockAndLockNextSegment(segmentCursor);
        }

        if (!moreToScan) {
            configured = false;
            // complete(Array.LockType.ReadLock);
        }

        index += ioDescriptor.valuesScanned();
        return ioDescriptor;
    }

    public void updateScan(byte[] buffer, int offset) {
        assert arrayDescriptor.isFixedLength();
        update(buffer, offset, arrayDescriptor.typeSize());
        throw new RuntimeException("Not supported");
    }

    public void updateScan(byte[] buffer, int offset, int size) {
        assert configured;
        int oldSize = arrayDescriptor.update(index, size);
        segmentCursor.update(buffer, offset, oldSize, size);
        throw new RuntimeException("Not supported");
        // complete(LockType.WriteLock);
    }

    public void update(byte[] buffer, int offset) {
        assert arrayDescriptor.isFixedLength();
        update(buffer, offset, arrayDescriptor.typeSize());
    }

    public void update(byte[] buffer, int offset, int size) {
        assert configured;
        int oldSize = arrayDescriptor.update(index, size);
        segmentCursor.update(buffer, offset, oldSize, size);
        // complete(LockType.WriteLock);
    }

    public void append(byte[] buffer, int offset) {
        assert arrayDescriptor.isFixedLength();
        append(buffer, offset, arrayDescriptor.typeSize());
    }

    public void append(byte[] buffer, int offset, int size) {
        assert buffer.length - offset >= size && size > 0;
        arrayDescriptor.acquire();
        try {
            arrayDescriptor.append(size);
            position(arrayDescriptor.length() - 1, LockType.WriteLock);
        } finally {
            arrayDescriptor.release();
        }
        segmentCursor.append(buffer, offset, size);
        complete(LockType.WriteLock);
    }

    public void deleteScan() {
        
        int size = arrayDescriptor.delete(index);
        segmentCursor.delete(size);
        throw new RuntimeException("Not supported");
        // complete(LockType.WriteLock);
    }

    public void delete() {
        int size = arrayDescriptor.delete(index);
        segmentCursor.delete(size);
        // complete(LockType.WriteLock);
    }

    public void complete(LockType lt) {
        if (configured) {
            segmentController.unlockSegment(segmentCursor, lt);
            configured = false;
        }
    }

    public void commit() {
        arrayDescriptor.acquire();
        for (Segment s : arrayDescriptor.segments()) {
            s.commit();
        }
        arrayDescriptor.persist();
        arrayDescriptor.release();
    }

    public void configure(Predicate p, Transaction t) {
        configured = true;
    }
    
    public ArrayDescriptor descriptor() {
        return arrayDescriptor;
    }

    public int position() {
        return index;
    }

    public void position(int position, LockType lt) {
        long offset = arrayDescriptor.convertIndexToOffset(position);
        segmentController.findAndLockSegment(segmentCursor, lt, offset);
        index = position;
        configured = true;
    }

    public boolean hasMore() {
        return (index == arrayDescriptor.length() ? false : true);
    }

    public IODescriptor createIODescriptor(byte[] buffer, int offset) {
        return new IODescriptor(arrayDescriptor, index, buffer, offset);
    }
  
}
