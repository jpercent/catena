package syndeticlogic.catena.store;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

public interface Compressor extends Callable<Object> {
    void add(int offset, PageDescriptor page);
    void run();
    void releaseTarget();
    ByteBuffer getTarget();
    Object call() throws Exception;
}
