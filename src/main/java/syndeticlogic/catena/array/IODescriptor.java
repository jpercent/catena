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

import java.util.LinkedList;
import syndeticlogic.catena.array.SegmentCursor;
/**
 * @author <a href="mailto:james@empty-set.net">James Percent</a>
 */
public class IODescriptor {

    private final ValueSizeRecorder valueSizeRecorder; 
    private final byte[] buffer;
    private final int offset;
    
    public IODescriptor(ArrayDescriptor arrayDescriptor, int index, byte[] buffer, int offset) {
        
        if(arrayDescriptor.isFixedLength()) {
            this.valueSizeRecorder = new FixedLengthRecorder(arrayDescriptor.typeSize());
        } else {
            this.valueSizeRecorder = new VariableLengthRecorder(arrayDescriptor, index);
        }
        this.buffer = buffer;
        this.offset = offset;
    }

    public int size(int i) {
        return valueSizeRecorder.valueScannedSize(i);
    }
    
    public int valuesScanned() {
        return valueSizeRecorder.valuesScanned();
    }
    
    public int recordValuesScanned(SegmentCursor cursor) {
        return valueSizeRecorder.recordValuesScanned(cursor);
    }
    
    public int ioSize() {
        return buffer.length - offset;
    }
    
    public byte[] buffer() {
        return buffer;
    }
    
    public int offset() {
        return offset;
    }
        
    public interface ValueSizeRecorder {
        int valuesScanned();
        int recordValuesScanned(SegmentCursor cursor);
        int valueScannedSize(int i);
    }
    
    public class FixedLengthRecorder implements ValueSizeRecorder {
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
    
    public class VariableLengthRecorder implements ValueSizeRecorder {
        private ArrayDescriptor arrayDescriptor;
        private LinkedList<Integer> recordedSizes;
        private int start;
        
        public VariableLengthRecorder(ArrayDescriptor arrayDescriptor, int startIndex) {
            this.arrayDescriptor = arrayDescriptor;
            this.start = startIndex;
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
}