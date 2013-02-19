package syndeticlogic.catena.performance;

public class IORecord {
    public enum IOStrategy { Read, Write };
    private final long controllerId;
    private final IOStrategy strategy;
    private long lba;
    private int size;
    private long duration;
    
    public IORecord(long controllerId, IOStrategy strategy) {
        this.controllerId = controllerId;
        this.strategy = strategy;
        this.lba = -1;
        this.size = -1;
        this.duration = -1;
    }

    public long getControllerId() {
        return controllerId;
    }
    
    public IOStrategy getStrategy() {
        return strategy;
    }
    
    public long getDuration() {
        return duration;
    }
    
    public void setDuration(long duration) {
        this.duration = duration;
    }
    
    public long getLba() {
        return lba;
    }

    public void setLba(long lba) {
        this.lba = lba;
    }
    
    public int getSize() {
        return size;
    }
        
    public void setSize(int size) {
        this.size = size;
    }
}
