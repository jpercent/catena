package syndeticlogic.catena.codec;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import syndeticlogic.catena.codec.CodeHelper;
import syndeticlogic.catena.codec.Codec;
import syndeticlogic.catena.type.Type;

public class CodeHelperTest {

	@Test
	public void testBasicCodeHelper() {
		//assertEquals(24, SizeOf.sizeOf(Codec.Type.CODEABLE));
		CodeHelper coder = new CodeHelper(new Codec(null));
		coder.append(true);
		coder.append(false);
		coder.append(10);
		coder.append(1111111111111L);
		coder.append(3.14159F);
		coder.append(1.6180339887498948482045868343656381177203091798057D);
		coder.append("James Percent");
		coder.append((byte)0xf);
		coder.append('a');
		coder.append((short)0xff);
		coder.append(Type.BINARY);

		byte[] b = coder.encodeByteArray();
		coder.reset();
		List<Object> objs = coder.decode(b, 0, 11);
		assertEquals(11, objs.size());
		assertTrue(objs.get(0) instanceof Boolean);
		assertTrue(objs.get(1) instanceof Boolean);
		assertTrue(objs.get(2) instanceof Integer);
		assertTrue(objs.get(3) instanceof Long);
		assertTrue(objs.get(4) instanceof Float);
		assertTrue(objs.get(5) instanceof Double);
		assertTrue(objs.get(6) instanceof String);
		assertTrue(objs.get(7) instanceof Byte);
		assertTrue(objs.get(8) instanceof Character);
		assertTrue(objs.get(9) instanceof Short);
		assertEquals(true, ((Boolean)objs.get(0)).booleanValue());
		assertEquals(false, ((Boolean)objs.get(1)).booleanValue());
		assertEquals(10, ((Integer)objs.get(2)).intValue());
		assertEquals(1111111111111L, ((Long)objs.get(3)).longValue());
		assertEquals(3.14159F, ((Float)objs.get(4)).floatValue(), 0.0);
		assertEquals(1.6180339887498948482045868343656381177203091798057D, ((Double)objs.get(5)).doubleValue(), 0.0);
		assertTrue("James Percent".equals((String)objs.get(6)));
		assertEquals(0xf, ((Byte)objs.get(7)).byteValue());
		assertEquals(new Character('a'), ((Character)objs.get(8)));
		assertEquals((short)0xff, ((Short)objs.get(9)).shortValue());
		assertEquals(Type.BINARY, (Type)objs.get(10));
	}

}
