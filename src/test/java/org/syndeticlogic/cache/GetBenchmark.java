package org.syndeticlogic.cache;



import org.syndeticlogic.cache.Benchmark;
import org.syndeticlogic.cache.Cache;


/**
 * @version $Revision: 1.1 $
 * @author $Author: jeffdrost $
 */
public class GetBenchmark implements Benchmark
{

    private String   name;
    private Object[] keys;

    public GetBenchmark(String name, Object[] keys)
    {
        this.keys = keys;
        this.name = name;
    }


    public void run(Cache cache)
    {

        for (int i = 0; i < keys.length; i++)
        {
            cache.getObject(keys[i]);
        }
    }


    public String toString()
    {
        return name;
    }
}
