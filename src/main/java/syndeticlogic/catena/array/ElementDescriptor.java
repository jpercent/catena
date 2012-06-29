package syndeticlogic.catena.array;

import syndeticlogic.catena.utility.CompositeKey;

public class ElementDescriptor {
    public CompositeKey segmentId;
    public int segmentOffset;
    public int size;
    public long index;

    public ElementDescriptor(CompositeKey segmentId, int segmentOffset, int size, long index) {
        this.segmentId = segmentId;
        this.segmentOffset = segmentOffset;
        this.size = size;
        this.index = index;
    }
}
