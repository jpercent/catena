package syndeticlogic.catena.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;

import syndeticlogic.memento.Cache;
import syndeticlogic.memento.PinnableListener;
import syndeticlogic.memento.PinnableLruStrategy;
import syndeticlogic.memento.PolicyCache;
import syndeticlogic.memento.PolicyStrategy;
import syndeticlogic.catena.utility.ObservationManager;

import syndeticlogic.catena.store.PageManager;

public class PageFactory {
    public static enum BufferPoolMemoryType {
        Native, Java
    };

    public static enum PageDescriptorType {
        Synchronized, Unsynchronized
    };

    public static enum CachingPolicy {
        Uncached, Fifo, Lru, Lfu, PinnableLru
    };

    private static volatile PageFactory singleton;

    public synchronized static void configure(PageFactory factory) {
        if (singleton == null) {
            singleton = factory;
        }
    }

    public static PageFactory get() {
        if (singleton == null) {
            throw new RuntimeException("PageFactory has not been injected");
        }
        return singleton;
    }

    private final BufferPoolMemoryType memoryType;
    private final CachingPolicy cachePolicy;
    private final PageDescriptorType pageDescType;
    private final int retryLimit;
    
    public PageFactory(BufferPoolMemoryType bpt, CachingPolicy cst,
            PageDescriptorType pdt, int retryLimit) {
        this.memoryType = bpt;
        this.cachePolicy = cst;
        this.pageDescType = pdt;
        this.retryLimit = retryLimit;
        assert memoryType != null && cachePolicy != null
                && pageDescType != null && retryLimit > 0;
    }

    public PageDescriptor createPageDescriptor() {
        PageDescriptor pagedes = null;
        switch (pageDescType) {
        case Unsynchronized:
            pagedes = new PageDescriptor();
            break;
        case Synchronized:
            assert false;
            break;
        default:
            assert false;
        }
        return pagedes;
    }

    public PageManager createPageManager(List<String> files, int pageSize,
            int numPages) {

        ConcurrentLinkedQueue<ByteBuffer> freelist = new ConcurrentLinkedQueue<ByteBuffer>();
        HashMap<String, List<PageDescriptor>> pageSequences = new HashMap<String, List<PageDescriptor>>();

        for (int i = 0; i < numPages; i++) {
            switch (memoryType) {
            case Native:
                freelist.add(ByteBuffer.allocateDirect(pageSize));
                break;
            case Java:
                freelist.add(ByteBuffer.allocate(pageSize));
                break;
            default:
                assert false;
            }
        }

        try {
            if (files != null) {
                assert false;
                for (String file : files) {

                    FileInputStream fileIn = new FileInputStream(new File(file));
                    long size = fileIn.getChannel().size();
                    assert (size / pageSize + 1) > 0
                            && (size / pageSize + 1) < Integer.MAX_VALUE;

                    int pages = (int) (size / pageSize + 1);
                    List<PageDescriptor> pageSequence = new ArrayList<PageDescriptor>(
                            pages);
                    long offset = 0;

                    for (int i = 0; i < pages; i++) {
                        pageSequence.add(createPageDescriptor());
                        offset += pageSize;
                    }
                    pageSequences.put(file, pageSequence);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        ObservationManager observer = new ObservationManager(null);
        PageManager pageManager = new PageManager(this, observer, pageSequences, freelist, pageSize, retryLimit);
        if(this.cachePolicy != CachingPolicy.Uncached) {
            long cacheTimeoutMillis = 999999999999L;
            PolicyStrategy policyStrategy = createPolicyStrategy(numPages, cacheTimeoutMillis);
            assert policyStrategy instanceof PinnableListener;
            Cache policyCache = createCache(policyStrategy, "page-cache");
            
            PageCache pageCache = new PageCache(pageManager, policyCache, (PinnableListener)policyStrategy);
            observer.register(pageCache);
            policyStrategy.setEvictionListener(pageCache);
        }

        return pageManager;
    }

    public Cache createCache(PolicyStrategy ps, String string) {
        return new PolicyCache(ps, "page-cache");
    }

    public PolicyStrategy createPolicyStrategy(int size, long timeoutMillis) {
        PolicyStrategy ps = null;
        switch (cachePolicy) {
        case Fifo:
        case Lru:
        case Lfu:
            assert false;
            break;
        case PinnableLru:
            ps = new PinnableLruStrategy(null, size, timeoutMillis);
        }
        return ps;
    }
}
