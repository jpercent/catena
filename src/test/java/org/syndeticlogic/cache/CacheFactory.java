package org.syndeticlogic.cache;

import org.syndeticlogic.cache.Cache;



/**
 * @author <a href="mailto:jeff@shiftone.org">Jeff Drost</a>
 * @version $Revision: 1.7 $
 */
public interface CacheFactory
{
    Cache newInstance(String cacheName, long timeoutMilliSeconds, int maxSize);
}
