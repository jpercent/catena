package syndeticlogic.catena.utility;

import static org.junit.Assert.*;

import org.junit.Test;

import syndeticlogic.catena.store.PageFactory.CachingPolicy;
import syndeticlogic.catena.store.SegmentManager.CompressionType;

public class ConfigTest {

	@Test
	public void testConfig() {
		System.out.println(CompressionType.Null.name());
		Config c = new Config();
		assertTrue(c.getArrayRegistry() != null);
		assertTrue(c.getCachingPolicy() == CachingPolicy.PinnableLru);
		assertTrue(c.getValueFactory() != null);
		assertTrue(c.getProperties() != null);
	}
}
