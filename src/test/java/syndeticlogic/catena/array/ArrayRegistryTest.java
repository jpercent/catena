package syndeticlogic.catena.array;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import syndeticlogic.catena.array.ArrayDescriptor;
import syndeticlogic.catena.array.ArrayRegistry;
import syndeticlogic.catena.store.PageFactory;
import syndeticlogic.catena.store.PageManager;
import syndeticlogic.catena.store.SegmentManager;
import syndeticlogic.catena.store.SegmentManager.CompressionType;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;
import syndeticlogic.catena.utility.CompositeKey;
import syndeticlogic.catena.utility.PropertiesUtility;

public class ArrayRegistryTest {

	@Test
	public void test() throws Exception {
	    String prefix = null;
	  try {  
	      Properties p = PropertiesUtility.load(PropertiesUtility.CONFIG_PROPERTIES);
	      prefix = p.getProperty(PropertiesUtility.CONFIG_BASE_DIRECTORY);
	        
	      Codec.configureCodec(null);
		
		CompositeKey key = new CompositeKey();
        key.append(prefix);
		key.append(0);
		CompositeKey key1 = new CompositeKey();
		key1.append(prefix);
		key1.append(1);
		CompositeKey key2 = new CompositeKey();
		key2.append(prefix);
		key2.append(2);
		CompositeKey key3 = new CompositeKey();
		key3.append(prefix);
		key3.append(3);
		CompositeKey key4 = new CompositeKey();
		key4.append(prefix);
		key4.append(4);
		CompositeKey key5 = new CompositeKey();
		key5.append(prefix);
		key5.append(5);
		int retryLimit = 2;
        PageFactory pa = new PageFactory(
                PageFactory.BufferPoolMemoryType.Native,
                PageFactory.CachingPolicy.PinnableLru,
                PageFactory.PageDescriptorType.Unsynchronized, retryLimit);

       PageManager pm = pa.createPageManager(null, 4096, 4096);

       
       SegmentManager.configureSegmentManager(CompressionType.Null, pm);
       ArrayRegistry arrayRegistry = new ArrayRegistry(p);

		arrayRegistry.createArray(key, Type.BINARY);
		arrayRegistry.createArray(key1, Type.BOOLEAN);
		arrayRegistry.createArray(key2, Type.BYTE);
		arrayRegistry.createArray(key3, Type.INTEGER);
		arrayRegistry.createArray(key4, Type.LONG);
		arrayRegistry.createArray(key5, Type.CODEABLE);
	
        // Check that the array descriptors are created correctly
        checkArrayManager(key, key1, key2, key3, key4, key5, arrayRegistry);
		
		for(ArrayDescriptor a : arrayRegistry.arrayDescriptors()) {
		    a.persist();
		}
		
		pa = new PageFactory(
                PageFactory.BufferPoolMemoryType.Native,
                PageFactory.CachingPolicy.PinnableLru,
                PageFactory.PageDescriptorType.Unsynchronized, retryLimit);

       pm = pa.createPageManager(null, 4096, 4096);

       
       SegmentManager.configureSegmentManager(CompressionType.Null, pm);
       arrayRegistry = new ArrayRegistry(p);

       // Check that the array descriptors are created correctly
       checkArrayManager(key, key1, key2, key3, key4, key5, arrayRegistry);
		
	} finally{
        FileUtils.deleteDirectory(new File(prefix));
    }
	}

	void checkArrayManager(CompositeKey key, CompositeKey key1, CompositeKey key2, 
			CompositeKey key3, CompositeKey key4, CompositeKey key5, ArrayRegistry arrayRegistry) {
	
		assertTrue(arrayRegistry.arrayDescriptor(key) instanceof ArrayDescriptor);
		assertEquals(Type.BINARY, arrayRegistry.arrayDescriptor(key).type());
		assertEquals(key, arrayRegistry.arrayDescriptor(key).id());
		assertEquals(0, arrayRegistry.arrayDescriptor(key).size());
		assertEquals(0, arrayRegistry.arrayDescriptor(key).length());
		assertEquals(-1, arrayRegistry.arrayDescriptor(key).typeSize());
        assertEquals(false, arrayRegistry.arrayDescriptor(key).isFixedLength());
		
		assertTrue(arrayRegistry.arrayDescriptor(key1) instanceof ArrayDescriptor);
		assertEquals(Type.BOOLEAN, arrayRegistry.arrayDescriptor(key1).type());
		assertEquals(key1, arrayRegistry.arrayDescriptor(key1).id());
		assertEquals(0, arrayRegistry.arrayDescriptor(key1).size());
		assertEquals(0, arrayRegistry.arrayDescriptor(key1).length());
		assertEquals(1, arrayRegistry.arrayDescriptor(key1).typeSize());
	    assertEquals(true, arrayRegistry.arrayDescriptor(key1).isFixedLength());
	      
		assertTrue(arrayRegistry.arrayDescriptor(key2) instanceof ArrayDescriptor);
		assertEquals(Type.BYTE, arrayRegistry.arrayDescriptor(key2).type());
		assertEquals(key2, arrayRegistry.arrayDescriptor(key2).id());
		assertEquals(0, arrayRegistry.arrayDescriptor(key2).size());
		assertEquals(0, arrayRegistry.arrayDescriptor(key2).length());
		assertEquals(1, arrayRegistry.arrayDescriptor(key2).typeSize());
        assertEquals(true, arrayRegistry.arrayDescriptor(key2).isFixedLength());		
		
		assertTrue(arrayRegistry.arrayDescriptor(key3) instanceof ArrayDescriptor);
		assertEquals(Type.INTEGER, arrayRegistry.arrayDescriptor(key3).type());
		assertEquals(key3, arrayRegistry.arrayDescriptor(key3).id());
		assertEquals(0, arrayRegistry.arrayDescriptor(key3).size());
		assertEquals(0, arrayRegistry.arrayDescriptor(key3).length());
		assertEquals(4, arrayRegistry.arrayDescriptor(key3).typeSize());
        assertEquals(true, arrayRegistry.arrayDescriptor(key3).isFixedLength());		
		
		assertTrue(arrayRegistry.arrayDescriptor(key4) instanceof ArrayDescriptor);
		assertEquals(Type.LONG, arrayRegistry.arrayDescriptor(key4).type());
		assertEquals(key4, arrayRegistry.arrayDescriptor(key4).id());
		assertEquals(0, arrayRegistry.arrayDescriptor(key4).size());
		assertEquals(0, arrayRegistry.arrayDescriptor(key4).length());
		assertEquals(8, arrayRegistry.arrayDescriptor(key4).typeSize());
        assertEquals(true, arrayRegistry.arrayDescriptor(key4).isFixedLength());		
		
		assertTrue(arrayRegistry.arrayDescriptor(key5) instanceof ArrayDescriptor);
		assertEquals(Type.CODEABLE, arrayRegistry.arrayDescriptor(key5).type());
		assertEquals(key5, arrayRegistry.arrayDescriptor(key5).id());
		assertEquals(0, arrayRegistry.arrayDescriptor(key5).size());
		assertEquals(0, arrayRegistry.arrayDescriptor(key5).length());
		assertEquals(-1, arrayRegistry.arrayDescriptor(key5).typeSize());	
        assertEquals(false, arrayRegistry.arrayDescriptor(key5).isFixedLength());		
	}
}
