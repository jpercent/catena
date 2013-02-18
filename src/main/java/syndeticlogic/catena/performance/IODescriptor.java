package syndeticlogic.catena.performance;

public interface IODescriptor {
    public enum IOStrategy { Read, Write };
    IOStrategy getStrategy();
    int dataSize();
    boolean  performIO() throws Exception;
}
