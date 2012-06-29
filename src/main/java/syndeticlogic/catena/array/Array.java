/*
 *   Copyright 2010, 2011 James Percent
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
import syndeticlogic.catena.type.Predicate;
import syndeticlogic.catena.store.Segment;
import syndeticlogic.catena.utility.Transaction;

import syndeticlogic.catena.array.SegmentCursor;

/**
 * @author <a href="mailto:james@empty-set.net">James Percent</a>
 */
public class Array {
    public enum LockType {
        ReadLock, WriteLock
    };

    private static final Log log = LogFactory.getLog(Array.class);
    private ArrayDescriptor arrayDesc;
    private SegmentController sctrl;
    private SegmentCursor cursor;
    private long index;
    private boolean configured;

    Array(ArrayDescriptor desc, SegmentController sctrl) {
        assert desc != null;
        this.arrayDesc = desc;
        this.sctrl = sctrl;
        this.index = 0;
        this.cursor = new SegmentCursor();
    }

    public ArrayDescriptor descriptor() {
        return arrayDesc;
    }

    public void configure(Predicate p, Transaction t) {
    }

    public long position() {
        return index;
    }

    public void position(long position, LockType lt) {
        long offset = convertIndexToOffset(position);
        sctrl.findAndLockSegment(cursor, lt, offset);
        index = position;
        configured = true;
    }

    public boolean hasMore() {
        return (index == arrayDesc.length() ? false : true);
    }

    public ScanDescriptor scan(int n, byte[] buffer, int offset) {
        if (!configured) {
            return null;
        }

        if (log.isTraceEnabled()) {
            log.debug("pos=" + index);
        }

        ScanDescriptor scandes = new ScanDescriptor(arrayDesc, n);
        int size = buffer.length - offset;
        int accumulation = 0;
        boolean moreToScan = true;

        while (scandes.elements() < n && accumulation < size && moreToScan) {
            int scanSize = scandes.recordElementsScanned(cursor);
            int scanned = cursor.scan(buffer, offset, scanSize);
            assert scanSize == scanned;
            accumulation += scanned;
            offset += scanned;

            if (accumulation < size) {
                moreToScan = sctrl.unlockAndLockNextSegment(cursor);
            }
        }

        if (moreToScan && cursor.scanned()) {
            moreToScan = sctrl.unlockAndLockNextSegment(cursor);
        }

        if (!moreToScan) {
            configured = false;
            // complete(Array.LockType.ReadLock);
        }

        index += scandes.elements();
        return scandes;
    }

    public void updateScan(byte[] buffer, int offset) {
        assert arrayDesc.isFixedLength();
        update(buffer, offset, arrayDesc.typeSize());
        throw new RuntimeException("Not supported");
    }

    public void updateScan(byte[] buffer, int offset, int size) {
        assert configured;
        int oldSize = arrayDesc.update(index, size);
        cursor.update(buffer, offset, oldSize, size);
        throw new RuntimeException("Not supported");
        // complete(LockType.WriteLock);
    }

    public void update(byte[] buffer, int offset) {
        assert arrayDesc.isFixedLength();
        update(buffer, offset, arrayDesc.typeSize());
    }

    public void update(byte[] buffer, int offset, int size) {
        assert configured;
        int oldSize = arrayDesc.update(index, size);
        cursor.update(buffer, offset, oldSize, size);
        // complete(LockType.WriteLock);
    }

    public void append(byte[] buffer, int offset) {
        assert arrayDesc.isFixedLength();
        append(buffer, offset, arrayDesc.typeSize());
    }

    public void append(byte[] buffer, int offset, int size) {
        assert buffer.length - offset >= size && size > 0;
        arrayDesc.acquire();
        try {
            arrayDesc.append(size);
            position(arrayDesc.length() - 1, LockType.WriteLock);
        } finally {
            arrayDesc.release();
        }
        cursor.append(buffer, offset, size);
        complete(LockType.WriteLock);
    }

    public void deleteScan() {
        int size = arrayDesc.delete(index);
        cursor.delete(size);
        throw new RuntimeException("Not supported");
        // complete(LockType.WriteLock);
    }

    public void delete() {
        int size = arrayDesc.delete(index);
        cursor.delete(size);
        // complete(LockType.WriteLock);
    }

    public void complete(LockType lt) {
        if (configured) {
            sctrl.unlockSegment(cursor, lt);
            configured = false;
        }
    }

    public void commit() {
        arrayDesc.acquire();

        for (Segment s : arrayDesc.segments()) {
            s.commit();
        }
        arrayDesc.persist();
        arrayDesc.release();
    }

    private long convertIndexToOffset(long index) throws IndexOutOfBoundsException {
        if (arrayDesc.isFixedLength()) {
            return index * arrayDesc.typeSize();
        } else {
            throw new RuntimeException("unsupported");
        }
    }
}
