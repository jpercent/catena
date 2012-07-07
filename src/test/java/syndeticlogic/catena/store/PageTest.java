package syndeticlogic.catena.store;

import static org.junit.Assert.*;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import org.junit.Test;


import syndeticlogic.catena.store.Page;
import syndeticlogic.catena.store.PageFactory;
import syndeticlogic.catena.utility.FixedLengthArrayGenerator;
import syndeticlogic.catena.utility.Util;

public class PageTest {
	@Test
	public void testEqualsCompareTo() throws Exception {
	/*	String filename = Util.prefixToPath("target") + "pagedescriptortest";
		//int pageSize = 4096;
		int seed = 42;
		int elementSize = 8;
		int length = 313;
		//PageFactory pageSystemAssembler = new PageFactory(PageFactory.BufferPoolMemoryType.Native, PageFactory.CachingPolicy.PinnableLru);
		FixedLengthArrayGenerator gen = new FixedLengthArrayGenerator(filename, seed, elementSize, length);
		gen.generateFileArray();
		
		ByteBuffer buf1 = ByteBuffer.allocateDirect(1024);
		Page pdes2 = new Page();
		Page pdes1 = new Page();
		Page pdes = new Page();
		
		pdes2.setDataSegmentId("0");
        pdes1.setDataSegmentId("0");
        pdes.setDataSegmentId("0");
        
        pdes2.setOffset(2);
        pdes1.setOffset(1);
        pdes.setOffset(0);
        
        pdes.attachBuffer(buf1);
		pdes1.attachBuffer(buf1);
		pdes2.attachBuffer(buf1);

		assertEquals(-1, pdes.compareTo(pdes1));
		assertEquals(-1, pdes.compareTo(pdes2));
		assertEquals(0, pdes.compareTo(pdes));
		assertEquals(1, pdes1.compareTo(pdes));
		assertEquals(1, pdes2.compareTo(pdes));
	
		PriorityQueue<Page> p = new PriorityQueue<Page>();
		p.add(pdes2);
		p.add(pdes1);
		p.add(pdes);

		assertEquals(pdes, p.poll());
		assertEquals(pdes1, p.poll());
		assertEquals(pdes2, p.poll());
		assertEquals(null, p.poll());
		*/
	}
	
	@Test
	public void testReadWrite() throws Exception {
		
		ByteBuffer buf1 = ByteBuffer.allocateDirect(8192);
		System.out.println("buf capacity"+buf1.capacity());
		System.out.println("buf "+buf1.limit());
		System.out.println("buf "+buf1.position());
		System.out.println("buf "+buf1.remaining());
		//System.out.println("buf "+buf.);
		
		String filename = Util.prefixToPath("target") + "pagedescriptortest";
		int pageSize = 4096;
		int seed = 42;
		int elementSize = 8;
		int length = 313;
		FixedLengthArrayGenerator gen = new FixedLengthArrayGenerator(filename, seed, elementSize, length);
		gen.generateFileArray();
		
		//PageFactory pageSystemAssembler = new PageFactory(PageFactory.BufferPoolMemoryType.Native, PageFactory.CachingPolicy.PinnableLru);
		
		Page pdes = new Page();
		ByteBuffer buf = ByteBuffer.allocateDirect(pageSize);
		pdes.attachBuffer(buf);
		FileChannel chan = new RandomAccessFile(filename, "r").getChannel();
		chan.read(buf);
		buf.rewind();
		assertEquals(pageSize,buf.capacity());
		assertEquals(0, buf.position());
			
		byte[] bytebag = new byte[8];
		int pageOffset = 0;
		List<byte[]> bytes = gen.generateMemoryArray(length);
		
		for(int i=0; i < length; i++, pageOffset+=8) {
			pdes.read(bytebag, 0, pageOffset, elementSize);
			assertArrayEquals(bytebag, bytes.get(i));
			if(i == seed) {
				bytebag[0] = 0xd;
				bytebag[1] = 0xe;
				bytebag[2] = 0xa;
				bytebag[3] = 0xd;
				bytebag[4] = 0xb;
				bytebag[5] = 0xe;
				bytebag[6] = 0xe;				
				bytebag[7] = 0xf;
				pdes.write(bytebag, 0, pageOffset, elementSize);
			}
			for(int j = 0; j < elementSize; j++) 
				bytebag[j] = 0;
		}
		
		pdes.detachBuffer();
		
		Page pdes1 = new Page();
		pdes1.attachBuffer(buf);
		assertEquals(pageSize,buf.capacity());
		assertEquals(0, buf.position());
		
		pageOffset = 0;
		gen.reset();
		bytes = gen.generateMemoryArray(length);
		
		for(int i=0; i < length; i++, pageOffset+=8) {
			pdes1.read(bytebag, 0, pageOffset, elementSize);
			if(i == seed) {
				byte[] writeTest = {0xd,0xe,0xa,0xd,0xb,0xe,0xe,0xf};
				assertArrayEquals(bytebag, writeTest);
			} else {
				assertArrayEquals(bytebag, bytes.get(i));
			}
			
			if(i == seed+1) {
				bytebag[0] = 0xd;
				bytebag[1] = 0xe;
				bytebag[2] = 0xa;
				bytebag[3] = 0xd;
				bytebag[4] = 0xb;
				bytebag[5] = 0xe;
				bytebag[6] = 0xe;				
				bytebag[7] = 0xf;
				pdes1.write(bytebag, 0, pageOffset, elementSize);
			}				
			for(int j = 0; j < elementSize; j++) 
				bytebag[j] = 0;
		}
		pdes = null;
		
		Page pdes2 = new Page();
		pdes2.attachBuffer(buf);
		
		assertEquals(pageSize,buf.capacity());
		assertEquals(0, buf.position());
		
		pageOffset = 0;
		gen.reset();
		bytes = gen.generateMemoryArray(length);
		
		for(int i=0; i < length; i++, pageOffset+=8) {
			pdes2.read(bytebag, 0, pageOffset, elementSize);
			
			if(i == seed || i == seed+1) {
				byte[] writeTest = {0xd,0xe,0xa,0xd,0xb,0xe,0xe,0xf};
				assertArrayEquals(bytebag, writeTest);
			} else {
				assertArrayEquals(bytebag, bytes.get(i));
			}
			
			for(int j = 0; j < elementSize; j++) 
				bytebag[j] = 0;
		}
	}
	
	@Test
	public void testReadParameters() throws Exception {


		int pageSize = 64;
		int seed = 42;
		int elementSize = 63;
		int length = 2;
		
		String filename = Util.prefixToPath("target") + "pagedescriptortest";
		
		FixedLengthArrayGenerator gen = new FixedLengthArrayGenerator(filename, seed, elementSize, length);
		gen.generateFileArray();	
		int retryLimit = 2;
		PageFactory pageFactory = new PageFactory(PageFactory.BufferPoolMemoryType.Native, 
		        PageFactory.CachingPolicy.PinnableLru, PageFactory.PageDescriptorType.Unsynchronized, 
		        retryLimit);
		Page pdes = pageFactory.createPageDescriptor();
		ByteBuffer buf = ByteBuffer.allocateDirect(pageSize);		
		pdes.attachBuffer(buf);
		
		byte[] bytebag = new byte[elementSize];
		int pageOffset = 0;
		List<byte[]> bytes = gen.generateMemoryArray(length);
		
	     pdes.write(bytes.get(0), 0, 0, 63);
	     pdes.write(bytes.get(1), 0, 63, 1);
		
		int read = pdes.read(bytebag, 0, pageOffset, elementSize);
		assertEquals(63, read);
		assertArrayEquals(bytes.get(0), bytebag); 
		assertTrue(bytes.get(1)[0] != bytebag[0]);
		
		read = pdes.read(bytebag, 0,  63, 1);
		assertEquals(1, read);
		
		read = pdes.read(bytebag, 0,  64, 1);
		assertEquals(0, read);
		
		assertEquals(bytes.get(1)[0], bytebag[0]);
	}
}
