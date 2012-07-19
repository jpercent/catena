package syndeticlogic.catena.array;

public class FixedLengthRecorder implements ValueRecorder {
    private int size;
    private int scanned;
    
    public FixedLengthRecorder(int size) {
        this.size = size;
        this.scanned = 0;
    }
    
    public int recordValuesScanned(SegmentCursor cursor) {
        int remainingBytes = cursor.remaining();
        assert remainingBytes % size == 0;
        assert remainingBytes >= 0;
        int newValues = remainingBytes / size;
        scanned += newValues;
        return newValues * size;
    }
    
    public int valuesScanned() {
        return scanned;
    }
    
    public int valueScannedSize(int i) {
        if(i >= scanned) {
            return -1;
        } else {
            return size;
        }
    }
}
