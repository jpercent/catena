package org.syndeticlogic.cache;

import org.syndeticlogic.cache.Cache;
import org.syndeticlogic.cache.PinnableLruStrategy;
import org.syndeticlogic.cache.PolicyCache;
import org.syndeticlogic.cache.Pinnable;

/**
 * Class LruCacheTestCase
 *
 * @author <a href="mailto:james@empty-set.net">James Percent</a>
 * @version $Revision: 1.0 $
 */
public class PinnableLruCacheTestCase extends TestCaseBase
{
	public static class PinnableTest implements Pinnable {
		int pinned;
		Object arg;
		
		public PinnableTest(Object arg) {
			pinned = 1;
			this.arg = arg;
		}
		
		@Override
		public void pin() {
			pinned++;
		}

		@Override
		public void unpin() {
			pinned --;
		}

		@Override
		public boolean isPinned() {
			assert pinned >= 0;
			return (pinned >= 1 ? true : false);
		}
	}
	
	public static class PinnableLruTestFactory implements CacheFactory {
		public Cache newInstance(String name, long timeoutMilliSeconds, int maxSize) {
			PinnableLruStrategy p = new PinnableLruStrategy(null, maxSize, timeoutMilliSeconds);
			p.setUnpinned();
			return new PolicyCache(p, name); 
		}
	}

	public PinnableLruCacheTestCase(String name) {
		super(name);
	}

    /**
     * Method getCacheFactory
     */
    public CacheFactory getCacheFactory()
    {
        return new PinnableLruTestFactory();
    }


    /**
     * Method testPinnableLru5
     */
    public void testPinnableLru5() {	
        PinnableLruStrategy strategy = new PinnableLruStrategy(null, 5, 5000);
        PolicyCache cache = new PolicyCache(strategy, "lru");
        //PinnableTest test =  new PinnableTest(new Integer(123456789));
        cache.addUncachedObject("A", new PinnableTest(new Integer(123456789)));
        cache.addObject("B", new PinnableTest("123456789"));
        cache.addUncachedObject("C", new PinnableTest(new Long(123456789)));
        cache.addUncachedObject("D", new PinnableTest(Boolean.TRUE));
        cache.addObject("E", new PinnableTest(new Float(1.2345)));
        strategy.unpin("A");
        strategy.unpin("B");
        strategy.unpin("C");
        strategy.unpin("D");
        strategy.unpin("E");        
        assertEquals("EDCBA", strategy.dumpFifoKeys());
        assertEquals("ABCDE", strategy.dumpPinnableLruKeys());
        assertNotNull(cache.getObject("C"));
        assertEquals("EDCBA", strategy.dumpFifoKeys());
        assertEquals("ABDEC", strategy.dumpPinnableLruKeys());
        cache.addUncachedObject("F", new PinnableTest(new Object()));
        strategy.unpin("F");
        assertEquals("FEDCB", strategy.dumpFifoKeys());
        assertEquals("BDECF", strategy.dumpPinnableLruKeys());
    }


    /**
     * Method testReValue
     */
    public void testLru10()
    {
        PinnableLruStrategy strategy = new PinnableLruStrategy(null, 10, 500000);
        PolicyCache cache = new PolicyCache(strategy, "lru");

        assertEquals(true, cache.addUncachedObject("A", "A"));    //1
        assertEquals("", strategy.dumpPinnableLruKeys());
        assertEquals("A", strategy.dumpFifoKeys());
        
        assertEquals(true, cache.addUncachedObject("B", "B"));    //2
        assertEquals("", strategy.dumpPinnableLruKeys());
        assertEquals("BA", strategy.dumpFifoKeys());
        
        assertEquals(true, cache.addUncachedObject("C", "C"));    //3
        strategy.unpin("A");
        strategy.unpin("B");
        strategy.unpin("C");
        assertEquals("ABC", strategy.dumpPinnableLruKeys());
        assertEquals("CBA", strategy.dumpFifoKeys());
        
        assertEquals(true, cache.addObject("D", "D"));    //4
        assertEquals("ABC", strategy.dumpPinnableLruKeys());
        assertEquals("DCBA", strategy.dumpFifoKeys());
        strategy.unpin("D");
        assertEquals("ABCD", strategy.dumpPinnableLruKeys());
        assertEquals("DCBA", strategy.dumpFifoKeys());
        
        assertEquals(true, cache.addObject("E", "E"));    //5
        strategy.unpin("E");
        assertEquals("ABCDE", strategy.dumpPinnableLruKeys());
        assertEquals("EDCBA", strategy.dumpFifoKeys());
        
        assertEquals(true, cache.addObject("F", "F"));    //6
        strategy.unpin("F");
        assertEquals("ABCDEF", strategy.dumpPinnableLruKeys());
        assertEquals("FEDCBA", strategy.dumpFifoKeys());
        
        assertEquals(true, cache.addObject("G", "G"));    //7
        strategy.unpin("G");
        assertEquals("ABCDEFG", strategy.dumpPinnableLruKeys());
        assertEquals("GFEDCBA", strategy.dumpFifoKeys());
        assertEquals(true, cache.addObject("H", "H"));    //8
        strategy.unpin("H");
        assertEquals("ABCDEFGH", strategy.dumpPinnableLruKeys());
        assertEquals("HGFEDCBA", strategy.dumpFifoKeys());
        cache.addObject("I", "I");    //9
        strategy.unpin("I");
        assertEquals("ABCDEFGHI", strategy.dumpPinnableLruKeys());
        assertEquals("IHGFEDCBA", strategy.dumpFifoKeys());
        
        assertEquals(true, cache.addObject("J", "J"));    //10
        strategy.unpin("J");
        assertEquals("ABCDEFGHIJ", strategy.dumpPinnableLruKeys());
        assertEquals("JIHGFEDCBA", strategy.dumpFifoKeys());

        // this should bump out A
        assertEquals(true, cache.addObject("K", "K"));    //11
        assertEquals("BCDEFGHIJ", strategy.dumpPinnableLruKeys());
        assertEquals("KJIHGFEDCB", strategy.dumpFifoKeys());

        // observe the effect of getObject
        assertNotNull(cache.getObject("E"));
        assertEquals("BCDFGHIJE", strategy.dumpPinnableLruKeys());
        assertEquals("KJIHGFEDCB", strategy.dumpFifoKeys());
        
        strategy.unpin("K");
        assertEquals("BCDFGHIJKE", strategy.dumpPinnableLruKeys());
        assertEquals("KJIHGFEDCB", strategy.dumpFifoKeys());

        
        assertNotNull(cache.getObject("G"));
        assertNotNull(cache.getObject("C"));
        assertNotNull(cache.getObject("E"));
        assertNotNull(cache.getObject("J"));
        assertTrue(cache.addObject("J", "J"));
        assertTrue(cache.addObject("E", "E"));        
      
        assertEquals("BDFHIKGCJE", strategy.dumpPinnableLruKeys());
        assertEquals("KJIHGFEDCB", strategy.dumpFifoKeys());
        
        assertEquals(true, cache.addObject("L", "L"));    //12
        assertEquals(true, cache.addObject("M", "K"));    //13
        assertEquals(true, cache.addObject("N", "K"));    //14
        assertEquals(true, cache.addObject("O", "K"));    //15
        assertEquals(true, cache.addObject("P", "K"));    //16
        assertEquals(true, cache.addObject("Q", "K"));    //17
        assertEquals(true, cache.addObject("R", "K"));    //18
        assertEquals(true, cache.addObject("S", "K"));    //19
        assertEquals(true, cache.addObject("T", "K"));    //20
        assertEquals(true, cache.addObject("U", "K"));    //21
        assertEquals(false, cache.addObject("V", "K"));    //22
        assertEquals(false, cache.addObject("Z", "K"));    //22
        assertEquals(false, cache.addObject("W", "K"));    //22

        assertNotNull(cache.getObject("L"));        
        assertNotNull(cache.getObject("U"));        
        
        assertEquals("", strategy.dumpPinnableLruKeys());
        assertEquals("UTSRQPONML", strategy.dumpFifoKeys());
        
        strategy.unpin("L");
        strategy.unpin("M");
        strategy.unpin("N");
        strategy.unpin("O");
        strategy.unpin("P");
        strategy.unpin("Q");
        strategy.unpin("R");
        strategy.unpin("S");
        strategy.unpin("T");
        strategy.unpin("U");
                
        assertNotNull(cache.getObject("Q"));        
        try {
        assertEquals("MNOPRSTLUQ", strategy.dumpPinnableLruKeys());
        assertEquals("UTSRQPONML", strategy.dumpFifoKeys());
        } catch(Exception e) {
        	throw new RuntimeException(e);
        }
    }
}
