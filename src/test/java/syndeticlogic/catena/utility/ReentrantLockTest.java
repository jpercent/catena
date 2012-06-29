package syndeticlogic.catena.utility;

import static org.junit.Assert.*;

import java.util.concurrent.locks.ReentrantLock;
import org.junit.Test;

public class ReentrantLockTest {
	public ReentrantLock lock = new ReentrantLock();
	volatile boolean held = true;
	volatile boolean released = false;
	volatile boolean started = false;
	volatile boolean stopped = false;
	class BasicThread2 implements Runnable {
	    public void run() {
	        while (!started) ;
            assertEquals(true, lock.isLocked());
	        held = false;
	        while(!stopped) ;
	        
	        assertEquals(true, lock.isLocked());
	        released = true;
	    }
	}

	@Test
	public void testReentrantLockPerformance() {
	    Runnable runnable = new BasicThread2();
	    Thread thread = new Thread(runnable);
	    thread.start();
	    
		lock.lock();
		started = true;
		while(held) ;
		lock.lock();
		lock.unlock();
		stopped = true;
		while(!released);
		lock.unlock();
	}
}
