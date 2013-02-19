package syndeticlogic.catena.performance;

public interface IOExecutor {
    public IORecord getIORecord();
    public void setIORecord(IORecord ioRecord);
    boolean  performIO() throws Exception;
}
