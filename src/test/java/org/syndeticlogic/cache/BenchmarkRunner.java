package org.syndeticlogic.cache;

import org.syndeticlogic.cache.Cache;
import org.syndeticlogic.cache.FifoStrategy;
import org.syndeticlogic.cache.LfuStrategy;
import org.syndeticlogic.cache.LruStrategy;
import org.syndeticlogic.cache.PolicyCache;
import org.syndeticlogic.cache.ZeroCache;

import java.io.PrintStream;
import java.util.Random;


/**
 * @version $Revision: 1.15 $
 * @author <a href="mailto:jeff@shiftone.org">Jeff Drost</a>
 */
public class BenchmarkRunner
{

    private static final int      TIMEOUT_MS       = 60 * 60 * 1000;    // 1 hour
    private static final int      MILLION          = 1000000;
    private static final int      KEY_COUNT        = MILLION;
    private static final Object[] SEQUENCIAL_KEYS  = new Object[KEY_COUNT];
    private static final Object[] PSUDORANDOM_KEYS = new Object[KEY_COUNT];
    private Benchmark[]           benchmarks;
    private Cache[]               caches;

    public BenchmarkRunner() throws Exception
    {

        Random random = new Random(0);
        // jpercent: modified below
        assert false;
        
        for (int i = 0; i < SEQUENCIAL_KEYS.length; i++)
        {
            SEQUENCIAL_KEYS[i]  = new Integer(i);
            PSUDORANDOM_KEYS[i] = new Integer(random.nextInt());
        }

        Benchmark sequencialAdd    = new AddBenchmark("S-Add", SEQUENCIAL_KEYS);
        Benchmark randomGet        = new GetBenchmark("R-Get", PSUDORANDOM_KEYS);
        Benchmark sequencialGet    = new GetBenchmark("S-Get", SEQUENCIAL_KEYS);
        Benchmark sequencialRemove = new RemoveBenchmark("S-Rem", SEQUENCIAL_KEYS);

        benchmarks = new Benchmark[]
        {
            sequencialAdd, sequencialAdd, sequencialAdd,                              //
            sequencialGet, sequencialGet, sequencialGet,                              //
            randomGet, randomGet, randomGet,                                          //
            sequencialRemove, sequencialRemove, sequencialRemove
        };
        caches     = new Cache[]
        {
            new ZeroCache(),        //
            new PolicyCache(new FifoStrategy(null, KEY_COUNT, TIMEOUT_MS), "fifo"),        //
            new PolicyCache(new LruStrategy(null, KEY_COUNT, TIMEOUT_MS), "lru"),          //
            new PolicyCache(new LfuStrategy(null, KEY_COUNT, TIMEOUT_MS), "lfu")          //
        };
    }


    private final long now()
    {
        return System.currentTimeMillis();
    }


    public void run(PrintStream tsv)
    {

        for (int i = 0; i < benchmarks.length; i++)
        {
            tsv.print("\t" + benchmarks[i]);
        }

        tsv.println();

        for (int i = 0; i < caches.length; i++)
        {
            run(caches[i], tsv);
        }
    }


    public void run(Cache cache, PrintStream tsv)
    {

        long start;

        tsv.print(cache);
        tsv.print('\t');

        // -----------------------------------------
        for (int i = 0; i < benchmarks.length; i++)
        {
            start = now();

            benchmarks[i].run(cache);
            tsv.print(now() - start);
            tsv.print('\t');
        }

        tsv.println();
        cache.clear();
        Runtime.getRuntime().gc();
        Runtime.getRuntime().runFinalization();
    }


    //    new BenchmarkRunner(new MapCache(new LinkedHashMap())),
    public static void main(String[] args) throws Exception
    {

        PrintStream     tsv    = System.out;
        BenchmarkRunner runner = new BenchmarkRunner();

        runner.run(tsv);
    }
}
