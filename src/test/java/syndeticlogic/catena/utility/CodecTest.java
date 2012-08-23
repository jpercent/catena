package syndeticlogic.catena.utility;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;

public class CodecTest {
	Type t;
	Codec c;
	
	@Before
	public void setUp() {
		t = Type.BOOLEAN;
		c = new Codec(null);
	}

	@Test
	public void testEncodeDecodeTypes() {
		List<Type> types = new ArrayList<Type>();
		types.add(Type.BINARY);
		types.add(Type.BYTE);
		types.add(Type.CODEABLE);
		types.add(Type.INTEGER);
		types.add(Type.LONG);
		types.add(Type.FLOAT);
		types.add(Type.DOUBLE);
		types.add(Type.CODEABLE);
		types.add(Type.STRING);
		types.add(Type.BINARY);
		types.add(Type.TYPE);
		
		byte[] codedTypes = new byte[55];
		int offset = 14;
		assertEquals(6, c.encodeTypes(codedTypes, offset, types));
		types = c.decodeTypes(codedTypes, offset, 11);
		assertEquals(11, types.size());
		assertEquals(Type.BINARY, types.get(0));
		assertEquals(Type.BYTE, types.get(1));
		assertEquals(Type.CODEABLE, types.get(2));
		assertEquals(Type.INTEGER, types.get(3));
		assertEquals(Type.LONG , types.get(4));
		assertEquals(Type.FLOAT , types.get(5));
		assertEquals(Type.DOUBLE , types.get(6));
		assertEquals(Type.CODEABLE , types.get(7));
		assertEquals(Type.STRING , types.get(8));
		assertEquals(Type.BINARY , types.get(9));
		assertEquals(Type.TYPE, types.get(10));

	}
	
	@Test
	public void testEncodeDecodeByteBufferTypes() {
		List<Type> types = new ArrayList<Type>();
		types.add(Type.BINARY);
		types.add(Type.BYTE);
		types.add(Type.CODEABLE);
		types.add(Type.INTEGER);
		types.add(Type.LONG);
		types.add(Type.FLOAT);
		types.add(Type.DOUBLE);
		types.add(Type.CODEABLE);
		types.add(Type.STRING);
		types.add(Type.BINARY);
		types.add(Type.TYPE);
		
		ByteBuffer codedTypes = ByteBuffer.allocateDirect(55);
		codedTypes.position(14);
		
		assertEquals(5, c.encodeTypes(codedTypes, types));
		codedTypes.position(14);
		types = c.decodeTypes(codedTypes, 11);
		
		assertEquals(11, types.size());
		assertEquals(Type.BINARY, types.get(0));
		assertEquals(Type.BYTE, types.get(1));
		assertEquals(Type.CODEABLE, types.get(2));
		assertEquals(Type.INTEGER, types.get(3));
		assertEquals(Type.LONG , types.get(4));
		assertEquals(Type.FLOAT , types.get(5));
		assertEquals(Type.DOUBLE , types.get(6));
		assertEquals(Type.CODEABLE , types.get(7));
		assertEquals(Type.STRING , types.get(8));
		assertEquals(Type.BINARY , types.get(9));
		assertEquals(Type.TYPE, types.get(10));
	}

	@Test
	public void testEncodeDecode() {
		
		byte[] code = new byte[234];
		
		assertEquals(Type.TYPE.length(),c.encode(Type.CHAR,code, 3));
		assertEquals(Type.CHAR, c.decodeCodecType(code, 3));
		
		assertEquals(Type.BOOLEAN.length(),c.encode(true,code, 3));
		assertEquals(true, c.decodeBoolean(code, 3));

		assertEquals(Type.BYTE.length(),c.encode((byte)0xff,code, 21));
		assertEquals((byte)0xff, c.decodeByte(code, 21));

		assertEquals(Type.CHAR.length(),c.encode('c',code, 22));		
		assertEquals('c', c.decodeChar(code, 22));

		assertEquals(Type.SHORT.length(),c.encode((short)255,code, 23));
		assertEquals((short)255, c.decodeShort(code, 23));
		
		assertEquals(Type.INTEGER.length(),c.encode(3255,code, 25));
		assertEquals(3255, c.decodeInteger(code, 25));
		
		assertEquals(Type.LONG.length(),c.encode(38888255333L,code, 29));
		assertEquals(38888255333L, c.decodeLong(code, 29));

		assertEquals(Type.FLOAT.length(),c.encode(3.14159f,code, 0));
		assertEquals(3.14159f, c.decodeFloat(code, 0), 0.0);
		
		assertEquals(Type.DOUBLE.length(),c.encode(3.14159D,code, 0));
		assertEquals(3.14159D, c.decodeDouble(code, 0), 0.0);
		
		String s = new String("James Percent");
		assertEquals(Type.STRING.length()+s.getBytes().length, c.encode(s, code, 31));
		assertEquals("James Percent", c.decodeString(code, 31));
		
		assertEquals(Type.BINARY.length()+s.getBytes().length, c.encode(s.getBytes(),
				code, 30));
		/*assertArrayEquals(s.getBytes(),*/ 
		byte[] ret = c.decodeBinary(code, 30);//;);
		assertEquals(s.getBytes().length, ret.length);
		for(int i = 0; i < s.getBytes().length; i++) {
			System.out.println("original, encoded, decoded == "+(int)s.getBytes()[i]+","+
					code[34+i]+","+(int)ret[i]);
		}

	}
}
