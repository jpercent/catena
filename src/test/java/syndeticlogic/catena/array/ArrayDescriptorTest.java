package syndeticlogic.catena.array;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import syndeticlogic.catena.stubs.SegmentStub;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;
import syndeticlogic.catena.utility.CompositeKey;

import syndeticlogic.catena.array.ArrayDescriptor;

public class ArrayDescriptorTest {
    
    @Test
    public void valueTest() throws Exception {
        CompositeKey c = new CompositeKey();
        c.append("Bo don't know Jack");
        ArrayDescriptor.ValueDescriptor vd = new ArrayDescriptor.ValueDescriptor(c, 11, 22222, 22, 33);
        assertEquals(c, vd.segmentId);
        assertEquals(11, vd.segmentOffset);
        assertEquals(22222, vd.byteOffset);
        assertEquals(22, vd.index);
        assertEquals(33, vd.valueSize);
    }
    
    @Test
    public void testFixedLength() throws Exception {
        String sep = System.getProperty("file.separator");
        String prefix = "target"+sep+"arrayDesccriptorTest"+sep;

        try {
            FileUtils.forceDelete(new File(prefix));
        } catch (Exception e) {
        }

        FileUtils.forceMkdir(new File(prefix));
        
        Codec.configureCodec(null);
        
        CompositeKey key = new CompositeKey();
        key.append(prefix);
        
        ArrayDescriptor adesc = new ArrayDescriptor(key, Type.INTEGER, 1048576);
        SegmentStub stub = new SegmentStub();
        stub.size = 32;
        stub.name = "arrayDescTest";
        adesc.addSegment(adesc.nextId(), stub);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        
        //assertEquals(false,adesc.checkIntegrity());
        //adesc.addSegment(new CompositeKey(), stub);
        assertEquals(true, adesc.checkIntegrity());
        
        assertEquals(key,adesc.id());
        assertEquals(Type.INTEGER,adesc.type());
        assertEquals(8,adesc.length());
        assertEquals(32,adesc.size());
        assertEquals(prefix+"1",adesc.nextId().toString());
        assertEquals(prefix+"2",adesc.nextId().toString());
        
        byte[] encoded = ArrayDescriptor.encode(adesc);
        adesc = ArrayDescriptor.decode(encoded, 0);
        adesc.addSegment(new CompositeKey(), stub);
        
        assertEquals(key,adesc.id());
        assertEquals(Type.INTEGER,adesc.type());
        assertEquals(8,adesc.length());
        assertEquals(32,adesc.size());
         
        adesc.append(4);
        stub.size = 36;
        assertEquals(adesc.size(), 36);
        assertEquals(adesc.length(), 9);
        
        adesc.delete(3);
        stub.size = 32;
        assertEquals(8,adesc.length());
        assertEquals(32,adesc.size());
        assertEquals(prefix+"3",adesc.nextId().toString());
        
        adesc.persist();
        FileInputStream reader = FileUtils.openInputStream(new File(key.toString()+ArrayDescriptor.ARRAY_DESC_FILE_NAME));
        byte[] fileBytes = new byte[reader.available()];
        reader.read(fileBytes);
        adesc = ArrayDescriptor.decode(fileBytes, 0);
        assertEquals(8,adesc.length());
        assertEquals(prefix+"4",adesc.nextId().toString());
    }
    
    @Test
    public void testVarLength() throws Exception {
        String sep = System.getProperty("file.separator");
        String prefix = "target"+sep+"arrayDesccriptorTest"+sep;

        try {
            FileUtils.forceDelete(new File(prefix));
        } catch (Exception e) {
        }

        FileUtils.forceMkdir(new File(prefix));
        
        Codec.configureCodec(null);
        
        CompositeKey key = new CompositeKey();
        key.append(prefix);
        
        ArrayDescriptor adesc = new ArrayDescriptor(key, Type.BINARY, 1048576);
        SegmentStub stub = new SegmentStub();
        stub.size = 32;
        stub.name = "arrayDescTest";

        adesc.addSegment(adesc.nextId(), stub);
        adesc.append(16);
        adesc.append(16);
        key.append('1');
        assertEquals(true,adesc.checkIntegrity());
        
        assertEquals(key,adesc.id());
        assertEquals(Type.BINARY,adesc.type());
        assertEquals(2,adesc.length());
        assertEquals(32,adesc.size());
    
        byte[] encoded = ArrayDescriptor.encode(adesc);
        adesc = ArrayDescriptor.decode(encoded, 0);
        
        assertEquals(key,adesc.id());
        assertEquals(Type.BINARY,adesc.type());
        assertEquals(2,adesc.length());
        assertEquals(32,adesc.size());
    }
}
