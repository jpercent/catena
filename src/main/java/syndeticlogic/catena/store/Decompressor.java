package syndeticlogic.catena.store;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

public interface Decompressor extends Callable<Object> {	
	void run();
	ByteBuffer source();
	Object call() throws Exception;
}
