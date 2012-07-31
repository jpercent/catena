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
}