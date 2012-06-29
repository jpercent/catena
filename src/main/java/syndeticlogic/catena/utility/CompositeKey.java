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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import syndeticlogic.catena.type.Format;

import static syndeticlogic.catena.utility.KeyElement.BYTE_TYPE;
import static syndeticlogic.catena.utility.KeyElement.INT_TYPE;
import static syndeticlogic.catena.utility.KeyElement.LONG_TYPE;
import static syndeticlogic.catena.utility.KeyElement.STRING_TYPE;

public class CompositeKey implements Comparable<CompositeKey> {
    
    public static final int MAX_KEY_SIZE = 1024;
    public static final int NUM_ELEMENTS_SIZE = 2;
    private List<KeyElement> components;
    private int size;
      
    public static CompositeKey decode(byte[] rawKey) {
        if (rawKey.length > MAX_KEY_SIZE) {
            throw new RuntimeException("Key too big");
        }
        short num_elements = Format.unpackShort(rawKey, 0);

        CompositeKey k = new CompositeKey();
        int element_index = 0;
        int meta_index = NUM_ELEMENTS_SIZE;
        int decode_index = NUM_ELEMENTS_SIZE;
        int meta_shift = 0;

        while (element_index < num_elements) {
            if (element_index % 4 == 0) {
                meta_shift = 0;
                meta_index = decode_index;
                decode_index++;
                k.size++;
            }

            int code = (rawKey[meta_index] >>> meta_shift) & 0x3;
            int length = 0;
            if (code == BYTE_TYPE)
                length = 1;
            else if (code == INT_TYPE)
                length = 4;
            else if (code == LONG_TYPE)
                length = 8;
            else {
                assert code == STRING_TYPE;
                length = Format.unpackShort(rawKey, decode_index);
                decode_index += Format.SHORT_SIZE;
                k.size += 2;
            }
            k.components.add(new KeyElement((byte) code, rawKey, decode_index,
                    length));
            decode_index += length;
            k.size += length;
            meta_shift += 2;
            element_index++;
        } 
        assert element_index == num_elements && num_elements == k.components.size();
        return k;
    }

    public static byte[] encode(CompositeKey k) {
    	assert k.size == k.computeSize();
        byte[] ret = new byte[k.size];
        CompositeKey.encode(k, ret, 0);
        return ret;
    }
        
    public static byte[] encode(CompositeKey k, byte[] ret, int offset) {
                
        for (int i = offset; i < k.size; i++)
            ret[i] = 0;

        Format.packShort(ret, offset, (short)k.components.size());
        offset += NUM_ELEMENTS_SIZE;
        
        int element_index = 0;
        int meta_index = offset;
        int encode_index = offset;
        int meta_shift = 0;

        for (; element_index < k.components.size(); element_index++) {
            if (element_index % 4 == 0) {
                meta_shift = 0;
                meta_index = encode_index;
                encode_index++;
            }

            KeyElement element = k.components.get(element_index);
            ret[meta_index] |= element.code << meta_shift;
            if (element.code == STRING_TYPE) {
                short string_size = (short) element.data.length;
                assert ((int) string_size) == element.data.length
                        && element.data.length <= MAX_KEY_SIZE;
                Format.packShort(ret, encode_index, string_size);
                encode_index += 2;
            }
            for (int i = 0; i < element.data.length; i++, encode_index++)
                ret[encode_index] = element.data[i];

            meta_shift += 2;
        }
        assert element_index == k.components.size();
        return ret;
    }

    public CompositeKey() {
        components = new ArrayList<KeyElement>(10);
        size = NUM_ELEMENTS_SIZE;
    }
    
    public void reset() {
        components = new ArrayList<KeyElement>(10);
        size = NUM_ELEMENTS_SIZE;
    }

    public void append(CompositeKey key) {
        for (KeyElement i : key.components)
            append(i);
    }

    public void append(byte b) {
        append(new KeyElement(b));
    }

    public void append(Byte b) {
        append(new KeyElement(b));
    }

    public void append(int i) {
        append(new KeyElement(i));
    }

    public void append(Integer i) {
        append(new KeyElement(i));
    }

    public void append(long l) {
        append(new KeyElement(l));
    }

    public void append(Long l) {
        append(new KeyElement(l));
    }

    public void append(String s) {
        append(new KeyElement(s));
    }
    
    public void append(KeyElement e) {
    	int new_length;
    	if(components.size() % 4 == 0)
        	new_length = size + e.data.length + 1;
        else 
        	new_length = size + e.data.length;
        
        if(e.code == STRING_TYPE) 
        	new_length += 2;

        if (new_length > MAX_KEY_SIZE)
            throw new RuntimeException("Key too large");
        
        //System.out.println("old, new size =" +size+", "+new_length);
        size = new_length;
        components.add(e);
    }

    public List<Object> getKeyComponents() {
        List<Object> o = new ArrayList<Object>(components.size());
        for (KeyElement c : components) {
            if (c.code == BYTE_TYPE) {
                assert c.data.length == 1;
                o.add(new Byte((byte) c.data[0]));
            } else if (c.code == INT_TYPE) {
                assert c.data.length == Format.INT_SIZE;
                o.add(new Integer(Format.unpackInt(c.data, 0)));
            } else if (c.code == LONG_TYPE) {
                assert c.data.length == Format.LONG_SIZE;
                o.add(new Long(Format.unpackLong(c.data, 0)));
            } else if (c.code == STRING_TYPE) {
                o.add(new String(c.data, 0, c.data.length));
            }
        }
        return o;
    }

    public int computeSize() {
        int bytes = NUM_ELEMENTS_SIZE;
        for (int i = 0; i < components.size(); i++) {
            if (i % 4 == 0) {
                bytes += 1;
            }
            KeyElement e = components.get(i);
            bytes += e.data.length;
            if (e.code == STRING_TYPE) {
                bytes += 2;
            }
        }
        assert bytes <= MAX_KEY_SIZE;
        //System.out.println("size, bytes"+size+", "+bytes);
        assert bytes == size;
        return size;
    }
    
    @Override
    public int compareTo(CompositeKey o) {
        if (this.equals(o))
            return 0;

        if (o == null)
            return 1;

        int ret = 0;
        Iterator<KeyElement> me = components.iterator();
        Iterator<KeyElement> you = o.components.iterator();
        boolean done = false;

        while (!done) {
            assert me.hasNext() || you.hasNext();
            if (me.hasNext() && you.hasNext()) {
                KeyElement myNext = me.next();
                KeyElement yourNext = you.next();
                ret = myNext.compareTo(yourNext);
                if (ret != 0) {
                    done = true;
                }

            } else if (!me.hasNext() && you.hasNext()) {
                ret = -1;
                done = true;
            } else if (me.hasNext() && !you.hasNext()) {
                ret = 1;
                done = true;
            }
        }
        return ret;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((components == null) ? 0 : components.hashCode());
        result = prime * result + size;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null)
            return false;

        if (!(obj instanceof CompositeKey))
            return false;

        CompositeKey other = (CompositeKey) obj;
        if (components == null) {
            if (other.components != null)
                return false;
        } else if (!components.equals(other.components)) {
            return false;
        }
        
        if (size != other.size)
            return false;

        return true;
    }

    @Override
    public String toString() {
        String ret = "";
        List<Object> comps = getKeyComponents();
        for (Object o : comps)
            ret += o.toString();

        return ret;
    }
}
