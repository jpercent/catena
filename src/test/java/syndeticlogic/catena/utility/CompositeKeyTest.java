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

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import syndeticlogic.catena.utility.CompositeKey;

import static org.junit.Assert.*;

public class CompositeKeyTest {

    @Test
    public void getComponentsTest() throws Exception {
        CompositeKey k = new CompositeKey();
        k.append((byte)1);
        k.append(2);
        k.append((long)3);
        k.append("456");
        k.append(new Byte((byte)7));
        k.append(new Integer(8));
        k.append(new Long(9));        
        List<Object> data = k.getKeyComponents();
        Iterator<Object> i = data.iterator();

        assertTrue(i.hasNext());
        Object obj = i.next();
        assertTrue(obj instanceof Byte);
        assertEquals((byte)1, ((Byte)obj).byteValue());
        
        assertTrue(i.hasNext());
        obj = i.next();
        assertTrue(obj instanceof Integer);
        assertEquals(2, ((Integer)obj).intValue());
        
        assertTrue(i.hasNext());
        obj = i.next();
        assertTrue(obj instanceof Long);
        assertEquals(3, ((Long)obj).longValue());
        
        assertTrue(i.hasNext());
        obj = i.next();
        assertTrue(obj instanceof String);
        assertEquals("456", ((String)obj));
        
        assertTrue(i.hasNext());
        obj = i.next();
        assertTrue(obj instanceof Byte);
        assertEquals((byte)7, ((Byte)obj).byteValue());
        
        assertTrue(i.hasNext());
        obj = i.next();
        assertTrue(obj instanceof Integer);
        assertEquals(8, ((Integer)obj).intValue());
        assertTrue(i.hasNext());
        
        obj = i.next();
        assertTrue(obj instanceof Long);
        assertEquals(9, ((Long)obj).longValue());
        assertFalse(i.hasNext());
        k = new CompositeKey();
        k.append(3);
        k.append(2);
        CompositeKey k1 = new CompositeKey();
        k1.append(2);
        k.append(2);
        k.append(8L);
        k.append("james");
        k.append("likes");
        k.append("coding");
        k.append(8L);
        k.computeSize();

    }
    
    @Test
    public void encodeKeyTest() throws Exception {
        
        CompositeKey k = new CompositeKey();
        k.append((byte)1);
        k.append(2);
        k.append((long)3);
        k.append("456");
        k.append(new Byte((byte)7));
        k.append(new Integer(8));
        k.append(new Long(9));        

        byte[] rawKey = CompositeKey.encode(k);
        CompositeKey k1 = CompositeKey.decode(rawKey);
        
        List<Object> data = k1.getKeyComponents();
        assertEquals(35, k.computeSize());
        Iterator<Object> i = data.iterator();

        assertTrue(i.hasNext());
        Object obj = i.next();
        assertTrue(obj instanceof Byte);
        assertEquals((byte)1, ((Byte)obj).byteValue());
        
        assertTrue(i.hasNext());
        obj = i.next();
        assertTrue(obj instanceof Integer);
        assertEquals(2, ((Integer)obj).intValue());
        
        assertTrue(i.hasNext());
        obj = i.next();
        assertTrue(obj instanceof Long);
        assertEquals(3, ((Long)obj).longValue());
        
        assertTrue(i.hasNext());
        obj = i.next();
        assertTrue(obj instanceof String);
        assertEquals("456", ((String)obj));
        
        assertTrue(i.hasNext());
        obj = i.next();
        assertTrue(obj instanceof Byte);
        assertEquals((byte)7, ((Byte)obj).byteValue());
        
        assertTrue(i.hasNext());
        obj = i.next();
        assertTrue(obj instanceof Integer);
        assertEquals(8, ((Integer)obj).intValue());
        assertTrue(i.hasNext());
        
        obj = i.next();
        assertTrue(obj instanceof Long);
        assertEquals(9, ((Long)obj).longValue());
        assertFalse(i.hasNext());
        System.out.println("toSting = "+ k.toString());

    }

    @Test
    public void testBounds() throws Exception {
        boolean caught = false;
        try {
            byte[] key = new byte[CompositeKey.MAX_KEY_SIZE+1];
            CompositeKey.decode(key);
        } catch(Exception e) {
            caught = true;
        }
        assertTrue(caught);
       /* caught = false;
        try {
            byte[] key = new byte[CompositeKey.MAX_KEY_SIZE];
            Format.packShort();
            CompositeKey.decode(key);
        } catch(Exception e) {
            caught = true;
        }
        assertFalse(caught);
        */
        caught = false;
        byte b = 8;
        CompositeKey k = new CompositeKey();
        int i = 0;
        while(i < CompositeKey.MAX_KEY_SIZE - 207) {
            k.append(b);
        	i++;
        }

        try {
            k.append(b);
        } catch(Exception e) {
            caught = true;
        }
        assertTrue(caught);
        
    }
    
    @Test
    public void testEquals() throws Exception {
        CompositeKey k = new CompositeKey();
        k.append((byte)1);
        k.append(2);
        k.append((long)3);
        k.append("456");
        k.append(new Byte((byte)7));
        k.append(new Integer(8));
        k.append(new Long(9));        
        byte[] rawKey = CompositeKey.encode(k);
        CompositeKey k1 = CompositeKey.decode(rawKey);
        assertTrue(k.equals(k1));
        k1.append(133);
        assertFalse(k.equals(k1));
    }
    
    @Test
    public void testCompare() throws Exception {
        
        CompositeKey k = new CompositeKey();
        k.append((byte)1);
        k.append(2);
        k.append((long)3);
        k.append("456");
        k.append(new Byte((byte)7));
        k.append(new Integer(8));
        k.append(new Long(9));        
        byte[] rawKey = CompositeKey.encode(k);
        CompositeKey k1 = CompositeKey.decode(rawKey);
        assertTrue(k.compareTo(k1) == 0);
        k1.append(133);
        assertTrue(k.compareTo(k1) < 0);
        assertTrue(k1.compareTo(k) > 0);
        
    }
}
