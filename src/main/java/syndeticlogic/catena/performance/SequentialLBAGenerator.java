package syndeticlogic.catena.performance;

public class SequentialLBAGenerator implements LBAGenerator {
    protected int blockSize;
    protected long cursor;
    protected int lastIOSize;
    protected int skipSize;
    
    public SequentialLBAGenerator(int blockSize, int skipSize) {
        assert blockSize >= 0 && skipSize >= 0;
        this.blockSize = blockSize;
        this.skipSize = skipSize;
    }
    
    public long getNextOffset(int lastIOSize) {
        this.lastIOSize = lastIOSize;
        updateCursor();
        return cursor;
    }
   
    public int getBlockSize() {
        return blockSize;
    }

    public long getCursor() {
        return cursor;
    }
    
    protected void updateCursor() {
        cursor += lastIOSize+skipSize*blockSize;        
    }

    public int getLastBlockSize() {
        return lastIOSize;
    }

    public int getSkipSize() {
        return skipSize;
    }    
}
