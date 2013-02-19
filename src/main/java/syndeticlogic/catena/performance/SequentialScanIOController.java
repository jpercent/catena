package syndeticlogic.catena.performance;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import syndeticlogic.catena.performance.IORecord.IOStrategy;

public class SequentialScanIOController implements IOController {
    private final ByteBuffer buffer;
    private final LBAGenerator lbaGenerator;
    private final MemoryType memoryType;
    private final long id;
    private final int maxios;
    
    private int iocount;
    protected SynchronousFileChannelIOExecutor executor;
    
    public SequentialScanIOController(FileChannel channel, MemoryType memoryType, SequentialLBAGenerator generator, int iocount, int bufferSize) {
        this.memoryType = memoryType;
        this.lbaGenerator = generator;
        if(memoryType == MemoryType.Java) {
            buffer = ByteBuffer.allocate(bufferSize);
        } else {
            assert memoryType == MemoryType.Native;
            buffer = ByteBuffer.allocateDirect(bufferSize);
        }
        this.maxios = iocount;
        this.iocount = 0;
        this.id = 0;
        this.executor = new SynchronousFileChannelIOExecutor(channel, buffer); 
    }
    
    @Override
    public Long getId() {
        return id;
    }
    
    @Override
    public boolean notDone() {
        return (iocount < maxios ? false : true);
    }

    @Override
    public IOExecutor getNextIOExecutor() {
        iocount++;
        buffer.rewind();
        long nextLba = lbaGenerator.getNextOffset(buffer.remaining());
        IORecord ioRecord = new IORecord(id, IOStrategy.Read);
        ioRecord.setLba(nextLba);
        ioRecord.setSize(buffer.remaining());
        executor.setIORecord(ioRecord);
        return executor;
    }

    @Override
    public MemoryType getMemoryType() {
        return memoryType;
    }

}
