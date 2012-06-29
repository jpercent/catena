package syndeticlogic.catena.utility;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

public class NativeMemoryTest {
		
	@Test
	public void testNativeReadParameters() throws Exception {
		List<ByteBuffer> list = new LinkedList<ByteBuffer>();
		boolean outOfMemory = false;
		boolean readded = false;
		while(true) {
			try {
				list.add(ByteBuffer.allocateDirect(1048576));
				Thread.sleep(100);
				if(outOfMemory)
					readded = true;
			} catch(OutOfMemoryError e) {
				outOfMemory = true;
				Thread.sleep(500);
				list.remove(0);
			}
			if(readded)
				break;
		}
		assertTrue(readded);
	}	 
}
