package syndeticlogic.catena.store;

import syndeticlogic.catena.type.Type;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.HashMap;

public class SegmentManager {
    public enum CompressionType { Snappy, Null };
    
    public static synchronized void configureSegmentManager(
            CompressionType cType, PageManager pm) 
    {
        segmentManager = new SegmentManager(cType, pm);
    }

    public static synchronized SegmentManager get() {
        if (segmentManager == null) {
            throw new RuntimeException("SegmentManager has not been injected");
        }
        return segmentManager;
    }

    private static SegmentManager segmentManager;
    private CompressionType compressionType;
    private HashMap<String, Segment> files;
    private PageManager pageManager;
    //private int cores;

    private SegmentManager(CompressionType compressionType,
            PageManager pageManager) {
        this.compressionType = compressionType;
        this.pageManager = pageManager;
        this.files = new HashMap<String, Segment>();
        //cores = Runtime.getRuntime().availableProcessors();
        //pool = Executors.newFixedThreadPool(this.cores);
    }

    public synchronized Segment create(String filename, Type type)
            throws Exception {
        assert filename != null;
        File f = new File(filename);
        if (f.exists()) {
            throw new RuntimeException("file exists");
        }
        @SuppressWarnings("resource")
		RandomAccessFile file = new RandomAccessFile(filename, "rw");
        SegmentHeader header = new SegmentHeader(file.getChannel(), pageManager);
        header.load();
        SerializedObjectChannel channel = new SerializedObjectChannel(file.getChannel());
        Segment fm = new Segment(type, header, channel,
                new ReentrantReadWriteLock(), pageManager, filename);
        files.put(filename, fm);
        fm.pin();
        return fm;
    }

    @SuppressWarnings("resource")
	public synchronized Segment lookup(String filename) {
        assert filename != null;
        Segment fm = null;

        fm = files.get(filename);

        if (fm == null) {
            File f = new File(filename);
            if (!f.exists()) {
                throw new RuntimeException("file does not exist");
            }

            pageManager.createPageSequence(filename);
            RandomAccessFile file;
            try {
                file = new RandomAccessFile(filename, "rw");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            SegmentHeader header = new SegmentHeader(file.getChannel(),
                    pageManager);
            SerializedObjectChannel channel;
            if (compressionType == CompressionType.Snappy) {
                channel = new SnappyDecorator(file.getChannel(),
                        pageManager.pageSize(), true);
            } else {
                channel = new SerializedObjectChannel(file.getChannel());
            }
            fm = new Segment(new ReentrantReadWriteLock(), header, channel,
                    pageManager, filename);
            files.put(filename, fm);
        } else {
            assert filename.equals(fm.getQualifiedFileName());
        }
        fm.pin();
        return fm;
    }

    public synchronized void release(Segment segment) {
        assert files.containsKey(segment.getQualifiedFileName());
        segment.unpin();
    }
}
