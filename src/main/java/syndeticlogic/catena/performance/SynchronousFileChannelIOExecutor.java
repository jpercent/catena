package syndeticlogic.catena.performance;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import syndeticlogic.catena.performance.IORecord.IOStrategy;

public class SynchronousFileChannelIOExecutor implements IOExecutor {
    private static final long serialVersionUID = 1L;
    private final FileChannel channel;
    private final ByteBuffer buffer;
    private IORecord lastIO;
    
    public SynchronousFileChannelIOExecutor(FileChannel channel, ByteBuffer buffer) {
        this.channel = channel;
        this.buffer = buffer;
    }
    @Override
    public IORecord getIORecord() {
        return lastIO;
    }
    @Override
    public void setIORecord(IORecord iorecord) {
        lastIO = iorecord;
    }
    @Override
    public boolean performIO() throws Exception {
        int iobytes;
        boolean ret = true;
        long start = System.currentTimeMillis();
        if(lastIO.getStrategy() == IOStrategy.Write){ 
            iobytes = channel.write(buffer, lastIO.getLba());
        } else {
            iobytes = channel.read(buffer, lastIO.getLba());
        }
        lastIO.setDuration(System.currentTimeMillis() - start);
        if(lastIO.getSize() != iobytes) {
            lastIO.setSize(iobytes);
            ret = false;
        }
        return ret;
    }
}
