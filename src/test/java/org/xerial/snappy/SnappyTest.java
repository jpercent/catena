package org.xerial.snappy;

import static org.junit.Assert.*;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Test;

import syndeticlogic.catena.utility.FixedLengthArrayGenerator;
import syndeticlogic.catena.utility.Util;

public class SnappyTest {

	@Test
	public void testByteBufferCompression() throws Exception {
//		List<String> files = new LinkedList<String>();
		String fileName = Util.prefixToPath("target")+"snappytest";
		int seed = 32;
		int elementSize = 4;
		int length = 16384;
		
//		FileChannel channel = new RandomAccessFile(new File(fileName), "rw").getChannel();
		
		FixedLengthArrayGenerator gen = new FixedLengthArrayGenerator(fileName, seed, elementSize, length);
		List<byte[]> fileData = gen.generateMemoryArray(length);
		ByteBuffer uncompressed = ByteBuffer.allocateDirect(elementSize * length);
		
		for(byte[] element : fileData) {
			uncompressed.put(element);
		}
		uncompressed.rewind();
		int max = Snappy.maxCompressedLength(elementSize * length);
		ByteBuffer compressed = ByteBuffer.allocateDirect(max);
		System.out.println("Max = "+ max);
		//Snappy.getUncompressedLength(input, offset, limit)
		System.out.println("compress limit = "+compressed.limit());
		System.out.println("uncompressed "+uncompressed.position()+"limit = "+uncompressed.limit());
		int amount = Snappy.compress(uncompressed, compressed);
		System.out.println("Compress return int: "+amount);
		
		for(int i = 0; i < uncompressed.limit(); i++) {
			uncompressed.put((byte)0);
		}
		uncompressed.rewind();
		
		Snappy.uncompress(compressed, uncompressed);
		System.out.println("uncompress return int: "+amount);
		for(byte[] expected : fileData) {
			byte[] actual = new byte[elementSize];
			uncompressed.get(actual);
			assertArrayEquals(expected, actual);
		}
	}

}
