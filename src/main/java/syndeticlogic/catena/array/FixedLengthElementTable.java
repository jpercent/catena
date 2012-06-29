package syndeticlogic.catena.array;

import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

import syndeticlogic.catena.utility.CompositeKey;

import syndeticlogic.catena.store.Segment;

public class FixedLengthElementTable implements ElementTable {

    private TreeMap<CompositeKey, Segment> segments;
    private ReentrantLock arrayDescriptorLock;
    private int size;

    FixedLengthElementTable(TreeMap<CompositeKey, Segment> segment, ReentrantLock lock, int size) {
        this.segments = segment;
        this.arrayDescriptorLock = lock;
        this.size = size;
    }

    @Override
    public ElementDescriptor find(long index) {
        long bytePosition = index * size;
        long current = 0;
        long previousEnd = 0;
        ElementDescriptor element = null;
        arrayDescriptorLock.lock();
        try {
            for (Entry<CompositeKey, Segment> iter : segments.entrySet()) {
                Segment segment = iter.getValue();
                previousEnd = current;
                current += segment.size();
                if (current > bytePosition) {
                    int offset = (int) (bytePosition - previousEnd);
                    element = new ElementDescriptor(iter.getKey(), offset, size, index);
                }
            }
        } finally {
            arrayDescriptorLock.unlock();
        }

        return element;
    }

    @Override
    public int update(long index, int size) {
        assert this.size == size;
        return size;
    }

    @Override
    public void append(int size) {
        assert this.size == size;
    }

    @Override
    public int delete(long index) {
        return size;
    }

    @Override
    public void persist() {
    }
}
