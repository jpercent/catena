/*
 *   Author == James Percent (james@empty-set.net)
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

import static org.junit.Assert.*;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;


import syndeticlogic.catena.array.Array;
import syndeticlogic.catena.array.ArrayRegistry;
import syndeticlogic.catena.array.ScanDescriptor;
import syndeticlogic.catena.codec.Codec;
import syndeticlogic.catena.store.PageFactory;
import syndeticlogic.catena.store.PageManager;
import syndeticlogic.catena.store.Segment;
import syndeticlogic.catena.store.SegmentManager;
import syndeticlogic.catena.store.SegmentManager.CompressionType;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.type.TypeFactory;
import syndeticlogic.catena.utility.ArrayGenerator;
import syndeticlogic.catena.utility.CompositeKey;
import syndeticlogic.catena.utility.FixedLengthArrayGenerator;
import syndeticlogic.catena.utility.PropertiesUtility;
import syndeticlogic.catena.utility.Util;
import syndeticlogic.catena.utility.VariableLengthArrayGenerator;

public class ArrayTest {
    public static final Log log = LogFactory.getLog(ArrayTest.class);
    Properties p = new Properties();
    String sep = System.getProperty("file.separator");
    String prefix = "target" + sep + "arrayTest" + sep;
    CompositeKey key; 
    PageFactory pf;
    PageManager pm;
    ArrayRegistry arrayRegistry;
    Array array;
    int retryLimit = 2;

    @Before
    public void setup() throws Exception {
        p.setProperty(PropertiesUtility.CONFIG_BASE_DIRECTORY, prefix);
        p.setProperty(PropertiesUtility.SPLIT_THRESHOLD, "1048676");

        try {
            FileUtils.forceDelete(new File(prefix));
        } catch (Exception e) {
        }

        FileUtils.forceMkdir(new File(prefix));

        Codec.configureCodec(new TypeFactory());
        key = new CompositeKey();
        key.append(prefix);

        pf = new PageFactory(PageFactory.BufferPoolMemoryType.Java, 
                PageFactory.CachingPolicy.PinnableLru, PageFactory.PageDescriptorType.Unsynchronized, 
                retryLimit);

        pm = pf.createPageManager(null, 4096, 4096);
        SegmentManager.configureSegmentManager(CompressionType.Null, pm);
        arrayRegistry = new ArrayRegistry(p);
        arrayRegistry.createArray(key, Type.INTEGER);
        array = arrayRegistry.createArrayInstance(key);
    }

    public void reconfigure() {
        key = new CompositeKey();
        key.append(prefix);

        pf = new PageFactory(PageFactory.BufferPoolMemoryType.Java, 
                PageFactory.CachingPolicy.PinnableLru, PageFactory.PageDescriptorType.Unsynchronized, 
                retryLimit);

        pm = pf.createPageManager(null, 4096, 4096);
        SegmentManager.configureSegmentManager(CompressionType.Null, pm);
        arrayRegistry = new ArrayRegistry(p);
        array = arrayRegistry.createArrayInstance(key);
    }
    
    @Test
    public void basicTest() {
        byte[] buf = new byte[4];
        Codec.getCodec().encode(42, buf, 0);
        array.append(buf, 0);
        Codec.getCodec().encode(31, buf, 0);
        array.append(buf, 0);
        Codec.getCodec().encode(337, buf, 0);
        array.append(buf, 0);

        buf = new byte[12];
        array.position(0, Array.LockType.ReadLock);
        
        IODescriptor sdesc = array.scan(array.createIODescriptor(buf, 0));
        array.complete(Array.LockType.ReadLock);

        assertEquals(3, sdesc.valuesScanned());
        assertEquals(4, sdesc.size(0));
        assertEquals(4, sdesc.size(1));
        assertEquals(4, sdesc.size(2));

        int first = Codec.getCodec().decodeInteger(buf, 0);
        int second = Codec.getCodec().decodeInteger(buf, 4);
        int third = Codec.getCodec().decodeInteger(buf, 8);
        assertEquals(42, first);
        assertEquals(31, second);
        assertEquals(337, third);

        Codec.getCodec().encode(1023, buf, 4);
        array.position(2, Array.LockType.WriteLock);
        array.update(buf, 4);
        sdesc = array.scan(array.createIODescriptor(buf, 8));

        third = Codec.getCodec().decodeInteger(buf, 8);

        assertEquals(1, sdesc.valuesScanned());
        assertEquals(4, sdesc.size(0));
        assertEquals(1023, third);
        array.complete(Array.LockType.WriteLock);

        array.position(2, Array.LockType.WriteLock);
        array.delete();
        array.complete(Array.LockType.WriteLock);

        buf = new byte[12];
        array.position(0, Array.LockType.ReadLock);
        sdesc = array.scan(array.createIODescriptor(buf, 0));
        array.complete(Array.LockType.ReadLock);

        assertEquals(2, sdesc.valuesScanned());
        assertEquals(4, sdesc.size(0));
        assertEquals(4, sdesc.size(1));

        first = Codec.getCodec().decodeInteger(buf, 0);
        second = Codec.getCodec().decodeInteger(buf, 4);
        assertEquals(42, first);
        assertEquals(31, second);
    }

    @Test
    public void appendTest() {
        byte[] buf = new byte[4];
        byte[] total = new byte[1048576*4];
        Codec.getCodec().encode(0xffffffff, buf, 0);
        for(int i = 0; i < 1048576; i++) {
            array.append(buf, 0);
        }

        array.position(0, Array.LockType.ReadLock);
        IODescriptor sdesc = array.scan(array.createIODescriptor(total, 0));
        array.complete(Array.LockType.ReadLock);
        for(int i = 0; i < 1048576; i++) {
            for(int j = 0; j < 4; j++) {
                assertEquals(buf[j], total[4*i+j]);
            }
        }

        sdesc = array.scan(array.createIODescriptor(total, 0));
        assertEquals(null,sdesc);
        array.position(0, Array.LockType.ReadLock);
        sdesc = array.scan(array.createIODescriptor(total, 0));
        array.complete(Array.LockType.ReadLock);
        for(int i = 0; i < 1048576; i++) {
            for(int j = 0; j < 4; j++) {
                assertEquals(buf[j], total[4*i+j]);
            }
        }
    }

   @Test
    public void appendCommitTest() {

        byte[] buf = new byte[4];
        byte[] total = new byte[1048576 * 4];
        Codec.getCodec().encode(0xdeadbeef, buf, 0);
        for (int i = 0; i < 1048576; i++) {
            array.append(buf, 0);
        }

        array.position(0, Array.LockType.ReadLock);
        IODescriptor sdesc = array.scan(array.createIODescriptor(total, 0));
        array.complete(Array.LockType.ReadLock);
        array.commit();

        reconfigure();
        for (int i = 0; i < 1048576 * 4; i++) {
            total[i] = 0;
        }

        array.position(0, Array.LockType.ReadLock);
        sdesc = array.scan(array.createIODescriptor(total, 0));
        //sdesc.value();
        array.complete(Array.LockType.ReadLock);
        for (int i = 0; i < 1048576; i++) {
            for (int j = 0; j < 4; j++) {
                assertEquals(buf[j], total[4 * i + j]);
            }
        }
    }
    
   // @Test
    public void updateTest() {

        byte[] buf = new byte[4];
        byte[] total = new byte[1048576 * 4];
        HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
        map.put(0, 0xdeadbeef);
        map.put(1, 0xffffffff);
        map.put(2, 0xadaddeee);
        map.put(3, 0xabaddade);
        map.put(4, 0xabcdefab);
        
        for (int i = 0; i < 1048576; i++) {
            int r = i % 5;
            Codec.getCodec().encode(map.get(r), buf, 0);
            array.append(buf, 0);
        }
        array.position(0, Array.LockType.ReadLock);
        IODescriptor sdesc = array.scan(array.createIODescriptor(total, 0));

        for (int i = 0; i < 1048576; i++) {
            int r = i % 5;
            
            Codec.getCodec().encode(map.get(r), buf, 0);            
            for (int j = 0; j < 4; j++) {
                assertEquals(buf[j], total[i*4+j]);
            }
        }
        
        for (int i = 0; i < 1048576; i++) {
            int r = i % 5;
            if(r == 0) {
                Codec.getCodec().encode(map.get(1), buf, 0);
                array.position(i, Array.LockType.WriteLock);
                array.update(buf, 0);
                array.complete(Array.LockType.WriteLock);
            }
        }
        
        for(int i = 0; i < 1048576*4; i++) {
            total[i] = 0;
        }
        
        array.position(0, Array.LockType.ReadLock);
        sdesc = array.scan(array.createIODescriptor(total, 0));
        for (int i = 0; i < 1048576; i++) {
            int r = i % 5;
            if(r == 0) {
                Codec.getCodec().encode(map.get(1), buf, 0);            
            } else {
                Codec.getCodec().encode(map.get(r), buf, 0);            
            }
            System.out.println("I = "+i);            
            for (int j = 0; j < 4; j++) {
                assertEquals(buf[j], total[4*i+j]);
            }
        }
        array.complete(Array.LockType.ReadLock);
    }
    
        /*
        
                for (int j = 0; j < 4; j++) {
                    assertEquals(buf[j], total[4 * i + j]);
                }
            }
        }
    }
    
    @Test
    public void updateCommitTest() {

        /* array.commit();

        reconfigure();
        for (int i = 0; i < 1048576 * 4; i++) {
            total[i] = 0;
        }
*/
        
        /*
        byte[] buf = new byte[4];
        byte[] total = new byte[1048576 * 4];
        HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
        map.put(0, 0xdeadbeef);
        map.put(1, 0xffffffff);
        map.put(2, 0xadaddeee);
        map.put(3, 0xabaddade);
        map.put(4, 0xabcdefab);
        
        for (int i = 0; i < 1048576; i++) {
            int r = i % 5;
            Codec.getCodec().encode(map.get(r), buf, 0);
            array.append(buf, 0);
        }

        array.position(0, Array.LockType.ReadLock);
        ScanDescriptor sdesc = array.scan(1048576, total, 0);
        array.complete(Array.LockType.ReadLock);
        array.commit();

        reconfigure();
        for (int i = 0; i < 1048576 * 4; i++) {
            total[i] = 0;
        }

        array.position(0, Array.LockType.ReadLock);
        sdesc = array.scan(1048576, total, 0);
        array.complete(Array.LockType.ReadLock);
        for (int i = 0; i < 1048576; i++) {
            for (int j = 0; j < 4; j++) {
                assertEquals(buf[j], total[4 * i + j]);
                /*
                 * if(i % 1024 == 0) { System.out.println("total i, j "+i +
                 * ", "+4*i+": "+ Integer.toHexString(total[4*i])); }
                 *
            }
        }
*/
    
    
            /*  OLD
        CompositeKey key1 = new CompositeKey();
        int nextKey = 0;
        key1.append(key);
        key1.append(nextKey);
       
        Segment seg = SegmentManager.get().lookup(key1.toString());
        //assertEquals(1048576, seg.size());
        byte[] buf1 = new byte[4];
        for(int i = 0; i < 1048576; i++) {
            seg.scan(buf1, 0, i, 4);
            assertArrayEquals(buf, buf1);
            if(i == 1048576/4 || 
               i == 1048576/2||
               i == 1048576/4*3) {
                key1 = new CompositeKey();
                nextKey++;
                key1.append(key);
                key1.append(nextKey);
                seg = SegmentManager.get().lookup(key1.toString());
            }
        }

        /*assertEquals(3, sdesc.elements());
        assertEquals(4, sdesc.size(0));
        assertEquals(4, sdesc.size(1));
        assertEquals(4, sdesc.size(2));
        */
    
    
    public void testAppendScan() throws Exception {
        int seed = 37;
        int elementSize = 8;
        int elements = 314159;
        boolean flush = false;
        boolean fixedLengthData = true;
        boolean updates = false;
        appendUpdateScanTest(seed, elementSize, elements, flush, fixedLengthData, updates, CompressionType.Snappy);
    }

    public void appendUpdateScanTest(int seed, int elementSize, int elements, boolean flush, boolean fixedLengthData,
            boolean testUpdate, SegmentManager.CompressionType compressionType) throws Exception {

        Codec.configureCodec(new TypeFactory());

        PageFactory pa =
            new PageFactory(PageFactory.BufferPoolMemoryType.Native, PageFactory.CachingPolicy.PinnableLru,
                    PageFactory.PageDescriptorType.Unsynchronized, retryLimit);

        ArrayGenerator fagen = null;
        if (fixedLengthData)
            fagen = new FixedLengthArrayGenerator(seed, elementSize, elements);
        else
            fagen = new VariableLengthArrayGenerator(seed, elements);

        SegmentManager.configureSegmentManager(compressionType, pa.createPageManager(null, 4096, 4096));
        SegmentManager fsa = SegmentManager.get();

        Util.delete("target/filesTest");
        CompositeKey fileId = new CompositeKey();
        fileId.append("target/filesTest");
        Segment fm = fsa.create(fileId.toString(), Type.BINARY);
        assertTrue(fm != null);

        List<byte[]> bytes = fagen.generateMemoryArray(elements);
        int i = 0;
        byte[] last = null;
        int position = 0;
        for (byte[] element : bytes) {
            fm.append(element, 0, element.length);
            // System.out.println("I "+i+"Position "+ position);
            if (testUpdate && i % 2 == 1) {
                fm.update(last, 0, position, element.length, last.length);
                position += last.length;
                last = null;
            } else {
                position += element.length;
                last = element;
            }
            assertEquals(position, (int) fm.size());
            i++;
        }

        if (flush) {
            fm.commit();
            fm = null;
            SegmentManager.configureSegmentManager(compressionType, pa.createPageManager(null, 4096, 4096));
            fsa = SegmentManager.get();
            fm = fsa.lookup(fileId.toString());

        }
        assertEquals(position, (int) fm.size());
        long fileoffset = 0;
        i = 0;
        last = null;

        for (byte[] actual : bytes) {
            if (testUpdate && i % 2 == 1) {
                actual = last;
            }
            byte[] expected = new byte[actual.length];
            // System.out.println("I "+i+"Position "+ fileoffset);
            fm.scan(expected, 0, fileoffset, actual.length);
            fileoffset += actual.length;
            System.out.println("element = " + i);
            assertArrayEquals(actual, expected);
            last = actual;
            i++;
        }
    }
}