package syndeticlogic.catena.array;

import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ScanDescriptor {
    private static final Log log = LogFactory.getLog(ScanDescriptor.class);
    private ArrayDescriptor arrayDesc;
    private LinkedList<Integer> sizes;

    public ScanDescriptor(ArrayDescriptor arrayDesc, int n) {
        if(log.isTraceEnabled()) log.trace("scan created for "+n+" elements ");
        this.arrayDesc = arrayDesc;
        sizes = new LinkedList<Integer>();
    }
    
    public int elements() {
        return sizes.size();
    }
    
    public int size(int i) {
        if(i >= sizes.size()) {
            return -1;
        }
        return sizes.get(i).intValue();
    }
    
    public int recordElementsScanned(SegmentCursor cursor) {
        if (arrayDesc.isFixedLength()) {
            int remainingBytes = cursor.remaining();
            assert remainingBytes % arrayDesc.typeSize() == 0;
            int remainingElements = remainingBytes / arrayDesc.typeSize();

            for (int i = 0; i < remainingElements; i++) {
                sizes.add(new Integer(arrayDesc.typeSize()));
            }
            return remainingElements * arrayDesc.typeSize();
        } else {
            throw new RuntimeException("unsupported");
        }
    }
}
