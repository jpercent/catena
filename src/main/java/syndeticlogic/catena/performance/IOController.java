package syndeticlogic.catena.performance;

public interface IOController {
    boolean notDone();
    IODescriptor getNextIODescriptor();
}
