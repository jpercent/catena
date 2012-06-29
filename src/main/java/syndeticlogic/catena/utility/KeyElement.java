/*
 *    Author: James Percent (james@empty-set.net)
 *    Copyright 2010, 2011 James Percent
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package syndeticlogic.catena.utility;
import java.util.Arrays;

import syndeticlogic.catena.type.Format;

public class KeyElement implements Comparable<KeyElement> {
    
    public static final byte BYTE_TYPE = 0;
    public static final byte INT_TYPE = 1;
    public static final byte LONG_TYPE = 2;
    public static final byte STRING_TYPE = 3;
    
    public final byte code;
    public final byte[] data;
    
    public KeyElement(byte code, byte [] data, int offset)
    {
        assert data.length > offset;
        assert code >= BYTE_TYPE && code <= STRING_TYPE;
        
        this.code = code;
        this.data = new byte[data.length - offset];
        for(int i = 0; i < this.data.length; i++) 
            this.data[i] = data[offset + i];
    }
    
    public KeyElement(byte code, byte [] data, int offset, int length)
    {
        assert data.length >= offset + length;
        assert code >= BYTE_TYPE && code <= STRING_TYPE;
        
        this.code = code;
        this.data = new byte[length];
        for(int i = 0; i < this.data.length; i++) 
            this.data[i] = data[offset + i];
    }
    
    public KeyElement(byte b) {
        this.code =  BYTE_TYPE;
        this.data = new byte[1];
        this.data[0] = b;
    }
    
    public KeyElement(Byte b) {
        this.code = BYTE_TYPE;
        this.data = new byte[1];
        this.data[0] = b.byteValue();
    }
    
    public KeyElement(int i) {
        this.code = INT_TYPE;
        this.data = new byte[Format.INT_SIZE];
        Format.packInt(this.data, 0, i);
    }
    
    public KeyElement(Integer i) {
        this.code = INT_TYPE;
        this.data = new byte[Format.INT_SIZE];
        Format.packInt(this.data, 0, i.intValue());
    }
    
    public KeyElement(long l) {
        this.code = LONG_TYPE;
        this.data = new byte[Format.LONG_SIZE];
        Format.packLong(this.data, 0, l);
    }
    
    public KeyElement(Long l) {
        this.code = LONG_TYPE;
        this.data = new byte[Format.LONG_SIZE];
        Format.packLong(this.data, 0, l.longValue());
    }
    
    public KeyElement(String s) {
        this.code = STRING_TYPE;
        this.data = new byte[s.length()];
        byte[] sb = s.getBytes();
        for(int i = 0; i < this.data.length; i++) 
            this.data[i] = sb[i];
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + code;
        result = prime * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null)
            return false;

        if (!(obj instanceof KeyElement))
            return false;
       
        KeyElement other = (KeyElement) obj;
        if (code != other.code) 
            return false;
       
        if (!Arrays.equals(data, other.data)) 
            return false;

        return true;
    }

    @Override
    public int compareTo(KeyElement o) {
        if(this.equals(o))
            return 0;
        
        if(o == null)
            return 1;
        
        for (int i = 0; ;i++) {
            assert i != data.length || i != o.data.length;
            if(i >= data.length && i < o.data.length) { 
                return -1;
            } else if(i < data.length && i < o.data.length) {
                return 1;
            } else {
                if(data[i] < data[i]) 
                    return -1;
                else if (data[i] > data[i])
                    return 1;
                
           }
        }
    }
}
