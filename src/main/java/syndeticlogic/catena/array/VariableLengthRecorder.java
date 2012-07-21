package syndeticlogic.catena.array;

import java.util.LinkedList;

public class VariableLengthRecorder implements ValueRecorder {
    private ArrayDescriptor arrayDescriptor;
    private LinkedList<Integer> recordedSizes;
    private int start;
    
    public VariableLengthRecorder(ArrayDescriptor arrayDescriptor, int startIndex) {
        this.arrayDescriptor = arrayDescriptor;
        this.start = startIndex;
        this.recordedSizes = new LinkedList<Integer>();
    }
    
    public int recordValuesScanned(SegmentCursor cursor) {
        int remainingBytes = cursor.remaining();
        int scanSize = 0;
        for(int i = start + recordedSizes.size(); remainingBytes > 0; i++) {
            int nextValueSize = arrayDescriptor.valueSize(i);
            remainingBytes -= nextValueSize;
            if(remainingBytes < 0) {
                break;
            }
            recordedSizes.add(nextValueSize);
            scanSize += nextValueSize;
        }
        return scanSize;
    }
    
    public int valuesScanned() {
        return recordedSizes.size();
    }
    
    public int valueScannedSize(int i) {
        int ret = -1;
        if(i < recordedSizes.size()) {
            ret = recordedSizes.get(i);
        }
        return ret;
    }
}
