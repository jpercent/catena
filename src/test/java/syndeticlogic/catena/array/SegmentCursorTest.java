package syndeticlogic.catena.array;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;


import syndeticlogic.catena.array.SegmentCursor;
import syndeticlogic.catena.array.BinaryArray.LockType;
import syndeticlogic.catena.store.SegmentTest;
import syndeticlogic.catena.stubs.SegmentStub;

public class SegmentCursorTest {
    SegmentTest stest;
    
    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testSegmentCursor() {
        SegmentCursor scursor = new SegmentCursor();
        SegmentStub s = new SegmentStub();
        s.size = 3144;
        scursor.configure(s, 3133, LockType.WriteLock);
        assertEquals(s, scursor.segment());
        assertEquals(3133, scursor.offset());
        assertEquals(LockType.WriteLock, scursor.lockType());
        
        byte[] buf = new byte[111];
        scursor.append(buf, 0, 11);
        
        assertEquals(0, s.lastSegmentOffset);
        assertEquals(0, s.lastBufOffset);
        assertEquals(buf, s.lastBuf);
        assertEquals(11, s.lastLength);
        
        s.size = 3146;
        scursor.reconfigure(s, 3133);
        assertEquals(3133, scursor.offset());
        assertEquals(s, scursor.segment());
        assertEquals(LockType.WriteLock, scursor.lockType());
        scursor.configure(s, 3133, LockType.ReadLock);
        assertEquals(3133, scursor.offset());
        assertEquals(s, scursor.segment());
        assertEquals(LockType.ReadLock, scursor.lockType());
        
        scursor.scan(buf, 42, 13);
        assertEquals(3146, scursor.offset());
        assertEquals(3133, s.lastSegmentOffset);
        assertEquals(42, s.lastBufOffset);
        assertEquals(buf, s.lastBuf);
        assertEquals(13, s.lastLength);
    }
}
