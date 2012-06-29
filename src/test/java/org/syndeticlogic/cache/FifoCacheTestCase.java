package org.syndeticlogic.cache;

import org.syndeticlogic.cache.Cache;
import org.syndeticlogic.cache.FifoStrategy;
import org.syndeticlogic.cache.PolicyCache;
/**
 * Class FifoCacheTestCase
 *
 * @author <a href="mailto:jeff@shiftone.org">Jeff Drost</a>
 * @version $Revision: 1.2 $
 */
public class FifoCacheTestCase extends TestCaseBase {

	public static class FifoTestFactory implements CacheFactory {
		public Cache newInstance(String name, long timeoutMilliSeconds, int maxSize) {
			return new PolicyCache(new FifoStrategy(null, maxSize, timeoutMilliSeconds), "fifo");
		}
	}
	
    public FifoCacheTestCase(String name) {
        super(name);
    }
    
    public CacheFactory getCacheFactory() {
        return new FifoTestFactory();
    }

    public void testFifo() {
    	FifoStrategy strategy = new FifoStrategy(null, 10, 5000);
        PolicyCache cache = new PolicyCache(strategy, "fifo");
        
        cache.addObject("A", "A");    //1
        cache.addObject("B", "B");    //2
        cache.addObject("C", "C");    //3
        cache.addObject("D", "D");    //4
        cache.addObject("E", "E");    //5
        cache.addObject("F", "F");    //6
        cache.addObject("G", "G");    //7
        cache.addObject("H", "H");    //8
        cache.addObject("I", "I");    //9
        cache.addObject("J", "J");    //10
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());

        assertNotNull(cache.getObject("A"));
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("B"));
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("C"));
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("D"));
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("E"));
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());

        cache.addObject("K", "K");    //11
        assertEquals("KJIHGFEDCB", strategy.dumpFifoKeys());
        cache.addObject("L", "L");    //12
        assertEquals("LKJIHGFEDC", strategy.dumpFifoKeys());
        cache.addObject("M", "M");    //13
        assertEquals("MLKJIHGFED", strategy.dumpFifoKeys());
        cache.addObject("N", "N");    //14
        assertEquals("NMLKJIHGFE", strategy.dumpFifoKeys());
        cache.addObject("O", "O");    //15
        assertEquals("ONMLKJIHGF", strategy.dumpFifoKeys());
    }
}
