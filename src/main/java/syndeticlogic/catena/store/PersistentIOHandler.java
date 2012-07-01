package syndeticlogic.catena.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.codec.CodeHelper;
import syndeticlogic.catena.codec.Codec;
import syndeticlogic.catena.type.Type;
import org.xerial.snappy.Snappy;

// XXX - rewrite me
//+------------------------
//Header
//+0------------------------
//+ type
//+ numPages
//+ CRC
//+14------------------------
//Data Segment
//+15------------------------
//+ dataSegmentSize
//+ dataSegment1start
//...
//+N largest
//+M smallest
//EOF
public class PersistentIOHandler {
    private static final Log log = LogFactory.getLog(PersistentIOHandler.class);
    private static final int HEADER_SIZE = 23;
    private Type type;
    private FileChannel channel;
    private PageManager pageManager;
    private List<PageDescriptor> pages;
    private HashMap<Integer, Long> pageOffsets;
    private ExecutorService pool;
    private String segmentId;
    private int numPages;

    public PersistentIOHandler(Type type, FileChannel channel,
            PageManager pageManager, List<PageDescriptor> pageVector,
            ExecutorService pool, String segmentId) 
    {
        this.type = type;
        this.channel = channel;
        this.pageManager = pageManager;
        this.pages = pageVector;
        this.pool = pool;

        this.pageOffsets = new HashMap<Integer, Long>();
        this.segmentId = segmentId;
        this.numPages = 0;
    }

    public Type type() {
        return type;
    }

    public int unpackHeader(CodeHelper coder, ByteBuffer buff)
    {
        int dataSize = 0;
        buff.limit(HEADER_SIZE);
        int bytesRead = 0;
        try {
            bytesRead = channel.read(buff);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
        assert bytesRead == HEADER_SIZE;
        buff.rewind();
        List<Object> meta = coder.decode(buff, 4);

        type = (Type) meta.get(0);
        numPages = ((Integer) meta.get(1)).intValue();
        dataSize = ((Integer) meta.get(2)).intValue();
        long crc = ((Long) meta.get(3)).longValue();
        coder.reset();
        coder.append(type);
        coder.append(numPages);
        coder.append(dataSize);

        if (log.isTraceEnabled())
            log.trace(" type " + type + " numPages " + numPages);

        byte[] content = coder.encodeByteArray();
        CRC32 testcrc = new CRC32();
        testcrc.update(content, 0, content.length);
        long crcvalue = testcrc.getValue();

        assert crcvalue == crc;
        return dataSize;
    }

    public void writeHeader(int dataSize) {
        CodeHelper coder = Codec.getCodec().coder();
        coder.append(type);
        coder.append(numPages);
        coder.append(dataSize);
        
        byte[] content = coder.encodeByteArray();
        CRC32 crc = new CRC32();
        crc.update(content, 0, content.length);
        long crcvalue = crc.getValue();

        coder.append(crcvalue);
        ByteBuffer buffer = coder.encodeByteBuffer();
        buffer.rewind();

        if(log.isTraceEnabled()) log.trace("bytes written: " + buffer.limit());
        try {
            channel.write(buffer);
            channel.position(0L);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    public void load() throws IOException, InterruptedException, ExecutionException {

        if (numPages == 0) {
            assert pages.size() == 0;
            PageDescriptor page = pageManager.pageDescriptor(segmentId);
            pages.add(page);
            pageOffsets.put(0, (long) HEADER_SIZE);
            return;
        }

        List<Decompressor> decompressors = new LinkedList<Decompressor>();
        boolean emptyVector = (pages.size() == 0 ? true : false);
        Codec coder = Codec.getCodec();
        ByteBuffer size = ByteBuffer.allocate(Type.INTEGER.length());
        PageDescriptor page = null;
        boolean loadPage = false;

        if(log.isTraceEnabled()) log.trace(numPages+"empty vector "+emptyVector);

        for (int i = 0; i < numPages; i++) {

            size.rewind();
            pageOffsets.put(i, channel.position());
            //long pos = channel.position();
            channel.read(size);
            size.rewind();
            int compressedPageSize = coder.decodeInteger(size);
            if (emptyVector) {
                // no numPages are cached. load every single 1 of them
                page = pageManager.pageDescriptor(segmentId);
                pages.add(page);
                loadPage = true;
            } else {
                // this page could be in the cache. if it is, seek past it on
                // disk
                assert numPages == pages.size();
                page = pages.get(i);
                if (!page.isAssigned()) {
                    loadPage = true;
                } else {
                    channel.position(channel.position() + compressedPageSize);
                }
            }
            if (loadPage) {
                ByteBuffer compressedBuffer = ByteBuffer.allocateDirect(
                        Snappy.maxCompressedLength(pageManager.pageSize()));
                compressedBuffer.limit(compressedPageSize);
                int bytesRead = channel.read(compressedBuffer);
                compressedBuffer.rewind();
                assert bytesRead == compressedBuffer.limit();
                Decompressor dcr = SegmentManager.get().createDecompressor(
                        page, compressedBuffer);
                decompressors.add(dcr);
                loadPage = false;
            }
        }

        for(Future<Object> f : pool.invokeAll(decompressors)) {
            f.get();
            assert f.isDone();
        }
        //for (Decompressor d : decompressors) {
        //    d.run();
        //    d = null;
        //}
        decompressors = null;
        if(log.isTraceEnabled()) log.trace("pages.size() " + pages.size());
    }

    public synchronized void commit(int dataSize) throws IOException, InterruptedException,
            ExecutionException 
    {
        long fileoffset = 0;
        int dirtyIndex = 0;
        
        for (; dirtyIndex < pages.size(); dirtyIndex++) {
            fileoffset = pageOffsets.get(dirtyIndex);
            if (pages.get(dirtyIndex).isDirty())
                break;
        }

        if (dirtyIndex == pages.size())
            return;

        for (int i = dirtyIndex; i < pages.size(); i++)
            pageOffsets.remove(i);

        List<Compressor> compressors = new LinkedList<Compressor>();
        int total = 0;
        int offset = 0;
        int pageSize = pageManager.pageSize();
        Compressor compressor = SegmentManager.get().createCompressor(pageManager, pageSize);
        PageDescriptor page = null;
        for (int i = dirtyIndex; i < pages.size(); i++) {
            page = pages.get(i);
            compressor.add(offset, page);
            int amountAdded = page.limit() - total;
            total += page.limit();
            
            if (total > pageSize) {
                compressors.add(compressor);
                compressor = SegmentManager.get().createCompressor(pageManager,
                        pageSize);

                offset = amountAdded;
                compressor.add(offset, page);
                total = amountAdded;
                offset = 0;
            } else if (total == pageSize) {
                compressors.add(compressor);
                compressor = SegmentManager.get().createCompressor(pageManager,
                        pageSize);

                total = 0;
                offset = 0;
            } else {
                offset = 0;
                if (i + 1 == pages.size())
                    compressors.add(compressor);

            }
        }

        for(Future<Object> f : pool.invokeAll(compressors)) {
         f.get();
         assert f.isDone();
        }
        //for (Compressor c : compressors)
        //    c.run();

        numPages = pageOffsets.size();
        Codec coder = Codec.getCodec();
        ByteBuffer length = ByteBuffer.allocate(Type.INTEGER.length());
        channel.position(fileoffset);

        for (Compressor c : compressors) {
            ByteBuffer target = c.getTarget();
            pageOffsets.put(dirtyIndex, fileoffset);
            dirtyIndex++;
            fileoffset += (target.limit() + Type.INTEGER.length());
            coder.encode(target.limit(), length);
            length.rewind();
            channel.write(length);
            channel.write(target);
            length.rewind();
            target = null;

        }
        assert dirtyIndex == pageOffsets.size();
        numPages = pageOffsets.size();
        channel.position(0L);
        writeHeader(dataSize);
        channel.force(true);
        length = null;
    }
}
