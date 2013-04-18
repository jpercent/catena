/*
 *   Copyright 2010 - 2013 James Percent
 *   catena@syndeticlogic.net
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
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;


import syndeticlogic.catena.array.BinaryArray;
import syndeticlogic.catena.array.BinaryArray.LockType;
import syndeticlogic.catena.array.ArrayRegistry;
import syndeticlogic.catena.store.PageFactory;
import syndeticlogic.catena.store.PageManager;
import syndeticlogic.catena.store.SegmentManager;
import syndeticlogic.catena.store.SegmentManager.CompressionType;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;
import syndeticlogic.catena.utility.CompositeKey;
import syndeticlogic.catena.utility.PropertiesUtility;
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
    BinaryArray array;
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

        Codec.configureCodec(null);
        key = new CompositeKey();
        key.append(prefix);

        pf = new PageFactory(PageFactory.BufferPoolMemoryType.Java, 
                PageFactory.CachingPolicy.PinnableLru, PageFactory.PageDescriptorType.Unsynchronized, 
                retryLimit);

        pm = pf.createPageManager(null, 4096, 8192);
        SegmentManager.configureSegmentManager(CompressionType.Null, pm);
        arrayRegistry = new ArrayRegistry(p);
        arrayRegistry.createArray(key, Type.INTEGER);
        array = arrayRegistry.createArrayInstance(key);
    }

    public void reconfigure() {
        pf = new PageFactory(PageFactory.BufferPoolMemoryType.Java, 
                PageFactory.CachingPolicy.PinnableLru, PageFactory.PageDescriptorType.Unsynchronized, 
                retryLimit);

        pm = pf.createPageManager(null, 4096, 8192);
        SegmentManager.configureSegmentManager(CompressionType.Null, pm);
        arrayRegistry = new ArrayRegistry(p);
        System.out.println("key =--------------------- "+key.toString());
        array = arrayRegistry.createArrayInstance(key);
    }
    
    @Test
    public void basicVariableLengthArrayTest() {
        CompositeKey key = new CompositeKey();
        key.append(this.key);
        key.append(1);
        
        arrayRegistry.createArray(key, Type.BINARY);
        array = arrayRegistry.createArrayInstance(key);
        
        VariableLengthArrayGenerator vlag = new VariableLengthArrayGenerator(37, 13);
        List<byte[]> arrayValues = vlag.generateMemoryArray(300);
        assertEquals(0, array.position());
        
        for(byte[] value : arrayValues) {
            array.append(value,0, value.length);
        }
                
        assertEquals(300, array.position());
        assertFalse(array.hasMore());
        array.position(0, BinaryArray.LockType.ReadLock);
        for(byte[] value : arrayValues) {
            byte[] buffer = new byte[value.length];
            array.scan(array.createIODescriptor(buffer, 0));
            assertArrayEquals(value, buffer); 
        }
        array.complete(BinaryArray.LockType.ReadLock);

    }

    @Test
    public void updateVariableLengthArrayTest() {
        CompositeKey newKey = new CompositeKey();
        newKey.append(this.key);
        newKey.append(1);
        this.key = newKey;
        
        arrayRegistry.createArray(key, Type.BINARY);
        array = arrayRegistry.createArrayInstance(key);
        
        VariableLengthArrayGenerator vlag = new VariableLengthArrayGenerator(37, 13);
        List<byte[]> arrayValues = vlag.generateMemoryArray(300);
        Random r = new Random(337);
        int skip = r.nextInt() % 13;
        List<byte[]> skipList = new LinkedList<byte[]>();
        
        assertEquals(0, array.position());
        
        for(byte[] value : arrayValues) {
            array.append(value,0, value.length);
        }
        
        assertEquals(300, array.position());
        assertFalse(array.hasMore());
        
        for(int i = 0; i < arrayValues.size(); i++) {
            if(i % skip == 0) {
                byte[] updateBytes = new byte[r.nextInt(8192)+1];
                r.nextBytes(updateBytes);
                skipList.add(updateBytes);
                array.position(i, LockType.WriteLock);
                array.update(updateBytes, 0, updateBytes.length);
                array.complete(LockType.WriteLock);
            }
        }
        
        array.position(0, BinaryArray.LockType.ReadLock);
        for(int i = 0, j = 0; i < arrayValues.size(); i++) {
            byte[] value = arrayValues.get(i);
            if(i % skip == 0) {
                value = skipList.get(j);
                j++;
            }
            byte[] buffer = new byte[value.length];
            array.scan(array.createIODescriptor(buffer, 0));
            assertArrayEquals(value, buffer); 
        }
        array.complete(BinaryArray.LockType.ReadLock);

    }
    

    @Test
    public void deleteVariableLengthArrayTest() {
        CompositeKey key = new CompositeKey();
        key.append(this.key);
        key.append(1);
        this.key = key;
        
        arrayRegistry.createArray(key, Type.BINARY);
        array = arrayRegistry.createArrayInstance(key);
        VariableLengthArrayGenerator vlag = new VariableLengthArrayGenerator(37, 13);
        List<byte[]> arrayValues = vlag.generateMemoryArray(300);
        Random r = new Random(337);
        int skip = r.nextInt() % 13;
        
        assertEquals(0, array.position());
        
        for(byte[] value : arrayValues) {
            array.append(value,0, value.length);
        }
        
        assertEquals(300, array.position());
        assertFalse(array.hasMore());
        
        for(int i = 0; i < arrayValues.size(); i++) {
            if(i % skip == 0) {
                array.position(i, LockType.WriteLock);
                array.delete();
                array.complete(LockType.WriteLock);
                arrayValues.remove(i);
            }
        }
        
        array.position(0, BinaryArray.LockType.ReadLock);
        for(int i = 0; i < arrayValues.size(); i++) {
            byte[] value = arrayValues.get(i);
            byte[] buffer = new byte[value.length];
            array.scan(array.createIODescriptor(buffer, 0));
            assertArrayEquals(value, buffer); 
        }
        array.complete(BinaryArray.LockType.ReadLock);
        assertFalse(array.hasMore());
    }
    

    @Test
    public void updateDeleteVariableLengthArrayTest() {
        CompositeKey key = new CompositeKey();
        key.append(this.key);
        key.append(1);
        
        arrayRegistry.createArray(key, Type.BINARY);
        array = arrayRegistry.createArrayInstance(key);
        VariableLengthArrayGenerator vlag = new VariableLengthArrayGenerator(37, 13);
        List<byte[]> arrayValues = vlag.generateMemoryArray(300);
        Random r = new Random(337);
        int skip = r.nextInt() % 13;
        List<byte[]> skipList = new LinkedList<byte[]>();
        
        assertEquals(0, array.position());
        
        for(byte[] value : arrayValues) {
            array.append(value,0, value.length);
        }
        
        assertEquals(300, array.position());
        assertFalse(array.hasMore());
        
        for(int i = 0; i < arrayValues.size(); i++) {
            if(i % skip == 0) {
                if(i % 2 == 0) {
                    array.position(i, LockType.WriteLock);
                    array.delete();
                    array.complete(LockType.WriteLock);
                    arrayValues.remove(i);
                } else {
                    byte[] updateBytes = new byte[r.nextInt(8192)+1];
                    r.nextBytes(updateBytes);
                    skipList.add(updateBytes);
                    array.position(i, LockType.WriteLock);
                    array.update(updateBytes, 0, updateBytes.length);
                    array.complete(LockType.WriteLock);                    
                }
            }
        }
        
        array.position(0, BinaryArray.LockType.ReadLock);
        for(int i = 0, j = 0; i < arrayValues.size(); i++) {
            byte[] value = arrayValues.get(i);
            if(i % skip == 0 && i % 2 != 0) {
                value = skipList.get(j);
                j++;
            }
            
            byte[] buffer = new byte[value.length];
            array.scan(array.createIODescriptor(buffer, 0));
            assertArrayEquals(value, buffer); 
        }
        array.complete(BinaryArray.LockType.ReadLock);
        assertFalse(array.hasMore());
    }
    

    @Test
    public void commitAppendVariableLengthArrayTest() throws InterruptedException {
        CompositeKey newKey = new CompositeKey();
        newKey.append(this.key);
        newKey.append("1"+System.getProperty("file.separator"));
        key = newKey;
        arrayRegistry.createArray(key, Type.BINARY);
        array = arrayRegistry.createArrayInstance(key);
        VariableLengthArrayGenerator vlag = new VariableLengthArrayGenerator(37, 13);
        List<byte[]> arrayValues = vlag.generateMemoryArray(300);
        Random r = new Random(337);
        int skip = r.nextInt() % 13;
        List<byte[]> skipList = new LinkedList<byte[]>();
        
        assertEquals(0, array.position());
        for(byte[] value : arrayValues) {
            array.append(value,0, value.length);
        }
        
        assertEquals(300, array.position());
        assertFalse(array.hasMore());
        array.commit();
        reconfigure();
        array.position(0, BinaryArray.LockType.ReadLock);
        for(int i = 0, j = 0; i < arrayValues.size(); i++) {
            byte[] value = arrayValues.get(i);
            if(i % skip == 0 && i % 2 != 0) {
                value = skipList.get(j);
                j++;
            }
            
            byte[] buffer = new byte[value.length];
            array.scan(array.createIODescriptor(buffer, 0));
            assertArrayEquals(value, buffer); 
        }
        array.complete(BinaryArray.LockType.ReadLock);
        assertFalse(array.hasMore());
    }


    @Test
    public void commitUpdateVariableLengthArrayTest() {
        CompositeKey newKey = new CompositeKey();
        newKey.append(this.key);
        newKey.append("1"+System.getProperty("file.separator"));
        key = newKey;
        arrayRegistry.createArray(key, Type.BINARY);
        array = arrayRegistry.createArrayInstance(key);
        VariableLengthArrayGenerator vlag = new VariableLengthArrayGenerator(37, 13);
        List<byte[]> arrayValues = vlag.generateMemoryArray(300);
        Random r = new Random(337);
        int skip = r.nextInt() % 13;
        List<byte[]> updateList = new LinkedList<byte[]>();
        
        assertEquals(0, array.position());
        
        for(byte[] value : arrayValues) {
            array.append(value,0, value.length);
        }
        assertEquals(300, array.position());
        assertFalse(array.hasMore());
        //int cumdelta = 0;
        for(int i = 0; i < arrayValues.size(); i++) {
            if(i % skip == 0) {
                byte[] updateBytes = new byte[r.nextInt(8192)+1];
                r.nextBytes(updateBytes);
                updateList.add(updateBytes);
                array.position(i, LockType.WriteLock);
                
                //int delta = updateBytes.length - arrayValues.get(i).length;
                //cumdelta += delta;
                array.update(updateBytes, 0, updateBytes.length);
                array.complete(LockType.WriteLock);                    
            }   
        }
        array.commit();
        reconfigure();
        array.position(0, BinaryArray.LockType.ReadLock);
        for(int i = 0, j = 0; i < arrayValues.size(); i++) {
            byte[] value = arrayValues.get(i);
            if(i % skip == 0) {
                value = updateList.get(j);
                j++;
            }
            
            byte[] buffer = new byte[value.length];
            array.scan(array.createIODescriptor(buffer, 0));
            System.out.println("i = "+ i+" value.size = "+value.length);
            assertArrayEquals(value, buffer); 
            
        }
        array.complete(BinaryArray.LockType.ReadLock);
        assertFalse(array.hasMore());
    }

    @Test
    public void commitDeleteVariableLengthArrayTest() {
        CompositeKey newKey = new CompositeKey();
        newKey.append(this.key);
        newKey.append("1"+System.getProperty("file.separator"));
        key = newKey;
        arrayRegistry.createArray(key, Type.BINARY);
        array = arrayRegistry.createArrayInstance(key);
        VariableLengthArrayGenerator vlag = new VariableLengthArrayGenerator(37, 13);
        List<byte[]> arrayValues = vlag.generateMemoryArray(300);
        Random r = new Random(337);
        int skip = r.nextInt() % 13;
        
        assertEquals(0, array.position());
        
        for(byte[] value : arrayValues) {
            System.out.println(" boject size = "+value.length);
            array.append(value,0, value.length);
        }
        
        assertEquals(300, array.position());
        assertFalse(array.hasMore());
        
        for(int i = 0; i < arrayValues.size(); i++) {
            if(i % skip == 0) {
                array.position(i, LockType.WriteLock);
                array.delete();
                array.complete(LockType.WriteLock);
                arrayValues.remove(i);
            }
        }
        array.commit();
        reconfigure();

        array.position(0, BinaryArray.LockType.ReadLock);
        for(int i = 0; i < arrayValues.size(); i++) {
            byte[] value = arrayValues.get(i);
            byte[] buffer = new byte[value.length];
            array.scan(array.createIODescriptor(buffer, 0));
            System.out.println("i = "+ i+" value.size = "+value.length);
       
            assertArrayEquals(value, buffer); 
        }
        array.complete(BinaryArray.LockType.ReadLock);
        assertFalse(array.hasMore());
    }
    

    @Test
    public void commitUpdateDeleteVariableLengthArrayTest() {
        CompositeKey newKey = new CompositeKey();
        newKey.append(this.key);
        newKey.append("1"+System.getProperty("file.separator"));
        key = newKey;
        System.out.println("---------------------------------------key = "+key.toString());
        arrayRegistry.createArray(key, Type.BINARY);
        array = arrayRegistry.createArrayInstance(key);
        VariableLengthArrayGenerator vlag = new VariableLengthArrayGenerator(37, 13);
        List<byte[]> arrayValues = vlag.generateMemoryArray(300);
        Random r = new Random(337);
        int skip = r.nextInt() % 13;
        List<byte[]> updateList = new LinkedList<byte[]>();
        
        assertEquals(0, array.position());
        
        for(byte[] value : arrayValues) {
            array.append(value,0, value.length);
        }
        
        assertEquals(300, array.position());
        assertFalse(array.hasMore());
        
        for(int i = 0, alternator = 0; i < arrayValues.size(); i++) {
            if(i % skip == 0) {
                
                if(alternator % 2 == 0) {
                    array.position(i, LockType.WriteLock);
                    array.delete();
                    array.complete(LockType.WriteLock);
                    arrayValues.remove(i);
                    
                } else {
                    byte[] updateBytes = new byte[r.nextInt(8192)+1];
                    r.nextBytes(updateBytes);
                    updateList.add(updateBytes);
                    array.position(i, LockType.WriteLock);
                    array.update(updateBytes, 0, updateBytes.length);
                    array.complete(LockType.WriteLock);                    
                }
                alternator++;
            }
        }
        
        array.commit();
        reconfigure();

        array.position(0, BinaryArray.LockType.ReadLock);
        for(int i = 0, j = 0, alternator = 0; i < arrayValues.size(); i++) {
            byte[] value = arrayValues.get(i);
            if(i % skip == 0) {
                if(alternator % 2 != 0) {
                    value = updateList.get(j);
                    j++;
                }
                alternator++;
            }
            
            byte[] buffer = new byte[value.length];
            array.scan(array.createIODescriptor(buffer, 0));
            System.out.println("i = "+ i+" value.size = "+value.length);
            assertArrayEquals(value, buffer); 
            
        }
        array.complete(BinaryArray.LockType.ReadLock);
        assertFalse(array.hasMore());
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
        array.position(0, BinaryArray.LockType.ReadLock);
        
        IODescriptor sdesc = array.scan(array.createIODescriptor(buf, 0));
        array.complete(BinaryArray.LockType.ReadLock);

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
        array.position(2, BinaryArray.LockType.WriteLock);
        array.update(buf, 4);
        sdesc = array.scan(array.createIODescriptor(buf, 8));

        third = Codec.getCodec().decodeInteger(buf, 8);

        assertEquals(1, sdesc.valuesScanned());
        assertEquals(4, sdesc.size(0));
        assertEquals(1023, third);
        array.complete(BinaryArray.LockType.WriteLock);

        array.position(2, BinaryArray.LockType.WriteLock);
        array.delete();
        array.complete(BinaryArray.LockType.WriteLock);

        buf = new byte[12];
        array.position(0, BinaryArray.LockType.ReadLock);
        System.out.println("size "+array.descriptor().length());
        sdesc = array.scan(array.createIODescriptor(buf, 0));
        array.complete(BinaryArray.LockType.ReadLock);

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

        array.position(0, BinaryArray.LockType.ReadLock);
        IODescriptor sdesc = array.scan(array.createIODescriptor(total, 0));
        array.complete(BinaryArray.LockType.ReadLock);
        for(int i = 0; i < 1048576; i++) {
            for(int j = 0; j < 4; j++) {
                assertEquals(buf[j], total[4*i+j]);
            }
        }

        sdesc = array.scan(array.createIODescriptor(total, 0));
        assertEquals(null,sdesc);
        array.position(0, BinaryArray.LockType.ReadLock);
        sdesc = array.scan(array.createIODescriptor(total, 0));
        array.complete(BinaryArray.LockType.ReadLock);
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

        array.position(0, BinaryArray.LockType.ReadLock);
        array.scan(array.createIODescriptor(total, 0));
        array.complete(BinaryArray.LockType.ReadLock);
        array.commit();

        reconfigure();
        for (int i = 0; i < 1048576 * 4; i++) {
            total[i] = 0;
        }

        array.position(0, BinaryArray.LockType.ReadLock);
        array.scan(array.createIODescriptor(total, 0));
        //sdesc.value();
        array.complete(BinaryArray.LockType.ReadLock);
        for (int i = 0; i < 1048576; i++) {
            for (int j = 0; j < 4; j++) {
                assertEquals(buf[j], total[4 * i + j]);
            }
        }
    }
    
   @Test
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
        array.position(0, BinaryArray.LockType.ReadLock);
        array.scan(array.createIODescriptor(total, 0));

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
                array.position(i, BinaryArray.LockType.WriteLock);
                array.update(buf, 0);
                array.complete(BinaryArray.LockType.WriteLock);
            }
        }
        
        for(int i = 0; i < 1048576*4; i++) {
            total[i] = 0;
        }
        
        array.position(0, BinaryArray.LockType.ReadLock);
        array.scan(array.createIODescriptor(total, 0));
        for (int i = 0; i < 1048576; i++) {
            int r = i % 5;
            if(r == 0) {
                Codec.getCodec().encode(map.get(1), buf, 0);            
            } else {
                Codec.getCodec().encode(map.get(r), buf, 0);            
            }
            
            for (int j = 0; j < 4; j++) {
                assertEquals(buf[j], total[4*i+j]);
            }
        }
        array.complete(BinaryArray.LockType.ReadLock);
    }
}