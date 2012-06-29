package org.syndeticlogic.cache;



import org.syndeticlogic.cache.Cache;
import org.syndeticlogic.cache.LfuStrategy;
import org.syndeticlogic.cache.PolicyCache;


/**
 * Class LfuCacheTestCase
 *
 * @author <a href="mailto:jeff@shiftone.org">Jeff Drost</a>
 * @version $Revision: 1.2 $
 */
public class LfuCacheTestCase extends TestCaseBase
{

	public static class LfuTestFactory implements CacheFactory {
		public Cache newInstance(String name, long timeoutMilliSeconds, int maxSize) {
			return new PolicyCache( new LfuStrategy(null, maxSize, timeoutMilliSeconds), name);
		}
	}
	
    public LfuCacheTestCase(String name)
    {
        super(name);
    }


    /**
     * Method getCacheFactory
     */
    public CacheFactory getCacheFactory()
    {
        return new LfuTestFactory();//null;//new LfuCacheFactory();
    }


    /**
     * Method testOne
     */
    public void testLfu10()
    {
    	
        LfuStrategy strategy = new LfuStrategy(null, 10, 5000);
        PolicyCache cache = new PolicyCache(strategy, "lfu");
        cache.addObject("A", "A");    //1
        assertEquals("A", strategy.dumpLfuKeys());
        assertEquals("A", strategy.dumpFifoKeys());
        cache.addObject("B", "B");    //2
        assertEquals("BA", strategy.dumpLfuKeys());
        assertEquals("BA", strategy.dumpFifoKeys());
        cache.addObject("C", "C");    //3
        assertEquals("CBA", strategy.dumpLfuKeys());
        assertEquals("CBA", strategy.dumpFifoKeys());
        cache.addObject("D", "D");    //4
        assertEquals("DCBA", strategy.dumpLfuKeys());
        assertEquals("DCBA", strategy.dumpFifoKeys());
        cache.addObject("E", "E");    //5
        assertEquals("EDCBA", strategy.dumpLfuKeys());
        assertEquals("EDCBA", strategy.dumpFifoKeys());
        cache.addObject("F", "F");    //6
        assertEquals("FEDCBA", strategy.dumpLfuKeys());
        assertEquals("FEDCBA", strategy.dumpFifoKeys());
        cache.addObject("G", "G");    //7
        assertEquals("GFEDCBA", strategy.dumpLfuKeys());
        assertEquals("GFEDCBA", strategy.dumpFifoKeys());
        cache.addObject("H", "H");    //8
        assertEquals("HGFEDCBA", strategy.dumpLfuKeys());
        assertEquals("HGFEDCBA", strategy.dumpFifoKeys());
        cache.addObject("I", "I");    //9
        assertEquals("IHGFEDCBA", strategy.dumpLfuKeys());
        assertEquals("IHGFEDCBA", strategy.dumpFifoKeys());
        cache.addObject("J", "J");    //10
        assertEquals("JIHGFEDCBA", strategy.dumpLfuKeys());
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());

        // observe the effect of getObject
        assertNotNull(cache.getObject("E"));
        assertEquals("EJIHGFDCBA", strategy.dumpLfuKeys());
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("H"));
        assertEquals("HEJIGFDCBA", strategy.dumpLfuKeys());
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("H"));
        assertEquals("HEJIGFDCBA", strategy.dumpLfuKeys());
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("B"));
        assertEquals("HBEJIGFDCA", strategy.dumpLfuKeys());
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("G"));
        assertEquals("HGBEJIFDCA", strategy.dumpLfuKeys());
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("G"));
        assertEquals("GHBEJIFDCA", strategy.dumpLfuKeys());
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("G"));
        assertEquals("GHBEJIFDCA", strategy.dumpLfuKeys());
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("E"));
        assertEquals("GEHBJIFDCA", strategy.dumpLfuKeys());
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("B"));
        assertEquals("GBEHJIFDCA", strategy.dumpLfuKeys());
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("I"));
        assertEquals("GBEHIJFDCA", strategy.dumpLfuKeys());
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());

        // observe the effect of adding more nodes
        cache.addObject("K", "K");
        assertEquals("GBEHIKJFDC", strategy.dumpLfuKeys());
        assertEquals("KJIHGFEDCB", strategy.dumpFifoKeys());
        cache.addObject("L", "L");
        assertEquals("GBEHILKJFD", strategy.dumpLfuKeys());
        assertEquals("LKJIHGFEDB", strategy.dumpFifoKeys());
        cache.addObject("M", "M");
        assertEquals("GBEHIMLKJF", strategy.dumpLfuKeys());
        assertEquals("MLKJIHGFEB", strategy.dumpFifoKeys());
        cache.addObject("N", "N");
        assertEquals("GBEHINMLKJ", strategy.dumpLfuKeys());
        assertEquals("NMLKJIHGEB", strategy.dumpFifoKeys());
        cache.addObject("O", "O");
        assertEquals("GBEHIONMLK", strategy.dumpLfuKeys());
        assertEquals("ONMLKIHGEB", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("O"));
        assertEquals("GBEHOINMLK", strategy.dumpLfuKeys());
        assertEquals("ONMLKIHGEB", strategy.dumpFifoKeys());
        cache.addObject("P", "P");
        assertEquals("GBEHOIPNML", strategy.dumpLfuKeys());
        assertEquals("PONMLIHGEB", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("P"));
        assertEquals("GBEHPOINML", strategy.dumpLfuKeys());
        assertEquals("PONMLIHGEB", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("P"));
        assertEquals("GPBEHOINML", strategy.dumpLfuKeys());
        assertEquals("PONMLIHGEB", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("P"));
        assertEquals("PGBEHOINML", strategy.dumpLfuKeys());
        assertEquals("PONMLIHGEB", strategy.dumpFifoKeys());
        assertNotNull(cache.getObject("P"));
        assertEquals("PGBEHOINML", strategy.dumpLfuKeys());
        assertEquals("PONMLIHGEB", strategy.dumpFifoKeys());
        cache.addObject("Q", "Q");
        assertEquals("PGBEHOIQNM", strategy.dumpLfuKeys());
        assertEquals("QPONMIHGEB", strategy.dumpFifoKeys());
    }
}
