package org.syndeticlogic.cache;

import junit.framework.TestCase;

import org.apache.commons.logging.LogFactory;
import org.syndeticlogic.cache.Cache;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;

/**
 * @version $Revision: 1.3 $
 * @author $Author: jeffdrost $
 */
public class ReaperTestCase extends TestCase {

	private static final Log LOG = LogFactory.getLog(ReaperTestCase.class);

	public void testSoft() throws Exception {

		CacheFactory factory = new FifoCacheTestCase.FifoTestFactory();
		Cache cache = factory.newInstance("test", 10000, 10000);

		LOG.info("cache = " + cache);
		cache.addObject("test", "test");
		Thread.sleep(5000);

		cache = null;

		Thread.sleep(5000);

		@SuppressWarnings("unused")
		Object o = createBigObject();
		
		Runtime.getRuntime().gc();
		Runtime.getRuntime().runFinalization();
		Thread.sleep(5000);
	}

	Object createBigObject() {

		List<byte[]> list = new ArrayList<byte[]>();

		for (int i = 0; i < 10000; i++) {
			list.add(new byte[1024]);
		}

		return list;
	}
}
