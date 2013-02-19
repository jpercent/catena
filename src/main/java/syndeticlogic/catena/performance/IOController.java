package syndeticlogic.catena.performance;

public interface IOController {
    public enum MemoryType { Java , Native };
    Long getId();
    MemoryType getMemoryType();
    boolean notDone();
    IOExecutor getNextIOExecutor();
}
