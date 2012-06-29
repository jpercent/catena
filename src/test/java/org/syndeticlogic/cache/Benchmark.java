package org.syndeticlogic.cache;




import org.syndeticlogic.cache.Cache;

import java.io.Serializable;


/**
 * @version $Revision: 1.1 $
 * @author $Author: jeffdrost $
 */
public interface Benchmark
{

    static final Serializable TEST_VALUE = "da' object";

    public void run(Cache cache);
}
