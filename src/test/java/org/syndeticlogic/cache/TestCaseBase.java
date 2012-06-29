package org.syndeticlogic.cache;

import junit.framework.TestCase;

import org.syndeticlogic.cache.AbstractPolicyStrategy;
import org.syndeticlogic.cache.Cache;
import org.syndeticlogic.cache.EvictionListener;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Class TestCaseBase
 * 
 * 
 * @author <a href="mailto:jeff@shiftone.org">Jeff Drost</a>
 * @version $Revision: 1.13 $
 */
public abstract class TestCaseBase extends TestCase implements EvictionListener {

	// / private static final Logger LOG = Logger.getLogger(TestCaseBase.class);
	static Random random = new Random(System.currentTimeMillis());
	private CacheFactory cacheFactory = getCacheFactory();
	private List<Cache.CacheNode> evicted;
	/**
	 * Constructor TestCaseBase
	 * 
	 * 
	 * @param name
	 */
	public TestCaseBase(String name) {
		super(name);
		evicted = new LinkedList<Cache.CacheNode>();
	}

	/**
	 * Method getCacheFactory
	 */
	public abstract CacheFactory getCacheFactory();

	/**
	 * Method newInstance
	 */
	public Cache newCache(long timeoutMilliSeconds, int maxSize) {
		return cacheFactory.newInstance("testCaseBase", timeoutMilliSeconds,
				maxSize);
	}

	/**
	 * Method test10000Puts
	 */
	public void test10000UniquePuts() {

		Cache cache = newCache(1000 * 60, 1000);

		for (int i = 0; i < 10000; i++) {
			cache.addObject("k-" + i, "v-" + i);
		}

		assertEquals(1000, cache.size());
		cache.clear();
		assertEquals(0, cache.size());
	}

	/**
	 * Method test10000Puts
	 */
	public void test10000Puts() {

		Cache cache = newCache(1000 * 60, 1000);

		for (int i = 0; i < 10000; i++) {
			cache.addObject("k-" + (i % 500), "v-" + i);
		}

		assertEquals(500, cache.size());
		cache.clear();
		assertEquals(0, cache.size());
	}

	/**
	 * Method test10000Puts
	 */
	public void test1000Puts10000GetsSameKey() {

		Cache cache = newCache(1000 * 60, 500);

		for (int i = 0; i < 1000; i++) {
			cache.addObject("k-" + i, "v-" + i);
		}

		assertEquals(500, cache.size());

		for (int i = 0; i < 10000; i++) {
			assertNotNull(cache.getObject("k-987"));
		}
	}

	/**
	 * Method test10000Puts99999Gets
	 */
	public void test10000Puts99999Gets() {

		Cache cache = newCache(1000 * 60, 1000);
		String key = null;

		cache.addObject("k-0", "v-0");

		for (int i = 0; i < 10000; i++) {
			key = "k-" + (i % 200);

			cache.addObject(key, "v-" + i);
			assertNotNull(key, cache.getObject(key));
		}

		assertEquals(200, cache.size());
	}

	/**
	 * Method test1Put10000GetsSameKey10Puts
	 */
	public void test1Put10000GetsSameKey2Puts() {

		Cache cache = newCache(1000 * 60, 2);

		cache.addObject("x", "x");
		cache.addObject("y", "y");

		for (int i = 0; i < 10000; i++) {
			assertNotNull(cache.getObject("x"));
			assertNotNull(cache.getObject("y"));
		}

		assertEquals(2, cache.size());
		cache.addObject("a", "a");
		cache.addObject("b", "b");
		assertEquals(2, cache.size());
		assertNotNull(cache.getObject("b"));
	}

	/**
	 * Method testCacheOfSize1
	 */
	public void testCacheOfSize1() {

		Cache cache = newCache(1000 * 60, 1);

		for (int i = 0; i < 20; i++) {
			cache.addObject("k-" + i, "v-" + i);
		}

		assertEquals(1, cache.size());
	}

	/**
	 * Method randKey
	 */
	public static String randKey() {
		return String.valueOf(Math.abs(random.nextInt()));
	}

	/**
	 * Method randValue
	 */
	public static String randValue() {

		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < 100; i++) {
			sb.append(randKey());
		}

		return sb.toString();
	}

	@Override
	public void evicted(Object userKey, Object cacheObject) {
		Cache.CacheNode node = new AbstractPolicyStrategy.BaseNode();
		node.setKey(userKey);
		node.setValue(cacheObject);
		evicted.add(node);
	}
}
