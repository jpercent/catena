package syndeticlogic.catena.utility;

import static org.junit.Assert.*;

import java.util.HashMap;

import org.junit.Test;

public class UtilityTest {
	@Test
	public void ByteArrayTest() {
		byte[] a = new byte[1];
		byte[] b = new byte[1];
		byte[] c = new byte[1];
		a[0] = 10;
		b[0] = 10;
		c[0] = 10;
		HashMap<byte[], Integer> hash = new HashMap<byte[], Integer>();
		hash.put(a, new Integer(0));
		hash.put(b, new Integer(1));
		
		assertEquals(1, hash.get(b).intValue());
		assertEquals(null, hash.get(c));
	}
	
}
