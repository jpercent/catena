package org.syndeticlogic.cache;



import junit.framework.TestCase;

import org.syndeticlogic.cache.Cache;

public class SimpleTestCase extends TestCase
{

    public void testSimple()
    {

        Cache  cache      = new LfuCacheTestCase.LfuTestFactory().newInstance("testSimple", 1000, 50);
        Object object     = new Object();
        String longString = "this is a string but it's not really that long";

        cache.addObject(longString, object);
        assertNotNull(cache.getObject(longString));
    }

    /*
        public void testSimpleNull()
        {

            Cache  cache      = new LfuCacheFactory().newInstance(1000, 50);
            Object object     = new Object();
            String longString = null;

            cache.addObject(null, null);

            cache.addObject(null, object);
            assertNotNull(cache.getObject(longString));
        }
        */
}
