package syndeticlogic.catena.array;


import static org.junit.Assert.*;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.*;

import syndeticlogic.catena.array.Array;
import syndeticlogic.catena.array.ArrayDescriptor;
import syndeticlogic.catena.array.SegmentController;
import syndeticlogic.catena.array.SegmentCursor;
import syndeticlogic.catena.codec.Codec;
import syndeticlogic.catena.stubs.SegmentStub;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.type.TypeFactory;
import syndeticlogic.catena.utility.CompositeKey;

public class SegmentControllerTest {
    
    @Test
    public void test() throws Exception {
        
        String sep = System.getProperty("file.separator");
        String prefix = "target"+sep+"segmentControllerTest"+sep;

        try {
            FileUtils.forceDelete(new File(prefix));
        } catch (Exception e) {
        }

        FileUtils.forceMkdir(new File(prefix));
        
        Codec.configureCodec(new TypeFactory());
        
        CompositeKey key = new CompositeKey();
        key.append(prefix);
        
        ArrayDescriptor adesc = new ArrayDescriptor(key, Type.INTEGER, 1048576);
        SegmentStub stub = new SegmentStub();
        
        CompositeKey nextId = adesc.nextId();
        adesc.addSegment(nextId, stub);
        stub.size = 32;
        stub.name = nextId.toString();
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        //adesc.addSegment(nextId, stub);
        
        SegmentStub stub1 = new SegmentStub();
        nextId = adesc.nextId();
        stub1.size = 36;
        stub1.name = nextId.toString();
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.append(4);
        adesc.addSegment(nextId, stub1);
        SegmentController sctrl = new SegmentController(adesc);
        SegmentCursor loc = new SegmentCursor();

        sctrl.findAndLockSegment(loc, Array.LockType.WriteLock, 8);
        assertEquals(stub, loc.segment());
        assertEquals(true, stub.writeLock);
        assertEquals(8, loc.offset());
        assertEquals(24, loc.remaining());

        
        
        boolean ret = sctrl.unlockAndLockNextSegment(loc);
        assertEquals(true, ret);
        assertEquals(false, stub.writeLock);
        assertEquals(stub1, loc.segment());
        assertEquals(true, stub1.writeLock);
        assertEquals(0, loc.offset());
        assertEquals(36, loc.remaining());
        ret = sctrl.unlockAndLockNextSegment(loc);
        
        assertEquals(false, ret);
        assertEquals(-1, loc.offset());
        assertEquals(-1, loc.remaining());
        assertEquals(null, loc.segment());
        
        
        sctrl.findAndLockSegment(loc, Array.LockType.WriteLock, 32);
        assertEquals(false, stub.writeLock);
        assertEquals(stub1, loc.segment());
        assertEquals(true, stub1.writeLock);
        assertEquals(0, loc.offset());
        assertEquals(36, loc.remaining());
    }
}
