package org.syndeticlogic.cache;

import org.syndeticlogic.cache.Cache;
import org.syndeticlogic.cache.LruStrategy;
import org.syndeticlogic.cache.PolicyCache;

/**
 * Class LruCacheTestCase
 *
 *
 * @author <a href="mailto:jeff@shiftone.org">Jeff Drost</a>
 * @version $Revision: 1.2 $
 */
public class LruCacheTestCase extends TestCaseBase
{

	public static class LruTestFactory implements CacheFactory {
		public Cache newInstance(String name, long timeoutMilliSeconds, int maxSize) {
			return new PolicyCache(new LruStrategy(null, maxSize, timeoutMilliSeconds), name);
		}
	}
	
    public LruCacheTestCase(String name)
    {
        super(name);
    }


    /**
     * Method getCacheFactory
     */
    public CacheFactory getCacheFactory()
    {
        return new LruTestFactory();
    }


    /**
     * Method testLru5
     */
    public void testLru5() {	
        LruStrategy strategy = new LruStrategy(null, 5, 5000);
        PolicyCache cache = new PolicyCache(strategy, "lru");
        
        cache.addObject("A", new Integer(123456789));
        cache.addObject("B", "123456789");
        cache.addObject("C", new Long(123456789));
        cache.addObject("D", Boolean.TRUE);
        cache.addObject("E", new Float(1.2345));
        assertEquals("EDCBA", strategy.dumpFifoKeys());
        assertEquals("EDCBA", strategy.dumpLruKeys());
        assertNotNull(cache.getObject("C"));
        assertEquals("EDCBA", strategy.dumpFifoKeys());
        assertEquals("CEDBA", strategy.dumpLruKeys());
        cache.addObject("F", new Object());
        assertEquals("FEDCB", strategy.dumpFifoKeys());
        assertEquals("FCEDB", strategy.dumpLruKeys());
    }


    /**
     * Method testReValue
     */
    public void testLru10()
    {
        LruStrategy strategy = new LruStrategy(null, 10, 5000);
        PolicyCache cache = new PolicyCache(strategy, "lru");

        cache.addObject("A", "A");    //1
        assertEquals("A", strategy.dumpLruKeys());
        assertEquals("A", strategy.dumpFifoKeys());
        cache.addObject("B", "B");    //2
        assertEquals("BA", strategy.dumpLruKeys());
        assertEquals("BA", strategy.dumpFifoKeys());
        cache.addObject("C", "C");    //3
        assertEquals("CBA", strategy.dumpLruKeys());
        assertEquals("CBA", strategy.dumpFifoKeys());
        cache.addObject("D", "D");    //4
        assertEquals("DCBA", strategy.dumpLruKeys());
        assertEquals("DCBA", strategy.dumpFifoKeys());
        cache.addObject("E", "E");    //5
        assertEquals("EDCBA", strategy.dumpLruKeys());
        assertEquals("EDCBA", strategy.dumpFifoKeys());
        cache.addObject("F", "F");    //6
        assertEquals("FEDCBA", strategy.dumpLruKeys());
        assertEquals("FEDCBA", strategy.dumpFifoKeys());
        cache.addObject("G", "G");    //7
        assertEquals("GFEDCBA", strategy.dumpLruKeys());
        assertEquals("GFEDCBA", strategy.dumpFifoKeys());
        cache.addObject("H", "H");    //8
        assertEquals("HGFEDCBA", strategy.dumpLruKeys());
        assertEquals("HGFEDCBA", strategy.dumpFifoKeys());
        cache.addObject("I", "I");    //9
        assertEquals("IHGFEDCBA", strategy.dumpLruKeys());
        assertEquals("IHGFEDCBA", strategy.dumpFifoKeys());
        cache.addObject("J", "J");    //10
        assertEquals("JIHGFEDCBA", strategy.dumpLruKeys());
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());

        // this should bump out A
        cache.addObject("K", "K");    //11
        assertEquals("KJIHGFEDCB", strategy.dumpLruKeys());
        assertEquals("KJIHGFEDCB", strategy.dumpFifoKeys());

        // observe the effect of getObject
        assertNotNull(cache.getObject("E"));
        assertEquals("EKJIHGFDCB", strategy.dumpLruKeys());
        assertEquals("KJIHGFEDCB", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("G"));
        assertEquals("GEKJIHFDCB", strategy.dumpLruKeys());
        assertEquals("KJIHGFEDCB", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("C"));
        assertEquals("CGEKJIHFDB", strategy.dumpLruKeys());
        assertEquals("KJIHGFEDCB", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("E"));
        assertEquals("ECGKJIHFDB", strategy.dumpLruKeys());
        assertEquals("KJIHGFEDCB", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("J"));
        assertEquals("JECGKIHFDB", strategy.dumpLruKeys());
        assertEquals("KJIHGFEDCB", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("J"));
        assertEquals("JECGKIHFDB", strategy.dumpLruKeys());
        assertEquals("KJIHGFEDCB", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("E"));
        assertEquals("EJCGKIHFDB", strategy.dumpLruKeys());
        assertEquals("KJIHGFEDCB", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("B"));
        assertEquals("BEJCGKIHFD", strategy.dumpLruKeys());
        assertEquals("KJIHGFEDCB", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("F"));
        assertEquals("FBEJCGKIHD", strategy.dumpLruKeys());
        assertEquals("KJIHGFEDCB", strategy.dumpFifoKeys());
    }
}
