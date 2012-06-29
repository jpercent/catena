package syndeticlogic.catena.utility;

import static org.junit.Assert.*;

import java.util.concurrent.locks.ReentrantLock;
import org.junit.Test;

public class LockPerformanceTest {
	public ReentrantLock lock = new ReentrantLock();
	public volatile int stackCallCount = 0;
	public int unwind = 8192;
	public int[] counts = new int[unwind];
	public int currentCount = 0;
	
	public int killtime() {
		currentCount = 0;
		for(int i = 0; i < 16384; i++) {
			currentCount += stackCallCount * 31 + currentCount * i;
		}
		return currentCount;
	}
	
	public void relock() {
		lock.lock();
		if(stackCallCount == unwind) {
			return;
		}
		stackCallCount++;
		counts[stackCallCount-1] = killtime();
		
		relock();
		lock.unlock();
	}
	
	
	public void lock() {
		if(stackCallCount == unwind) {
			return;
		}
		stackCallCount++;
		counts[stackCallCount-1] = killtime();
		
		lock();
	}

	@Test
	public void testReentrantLockPerformance() {
		long start = System.currentTimeMillis();
		lock.lock();
		relock();
		lock.unlock();
		long relockElapsed = System.currentTimeMillis() - start;
		stackCallCount = 0;
		System.out.println("relock elapsed time = "+relockElapsed);
		start = System.currentTimeMillis();
		lock.lock();
		lock();
		lock.unlock();
		long lockElapsed = System.currentTimeMillis() - start;
		System.out.println(" lock elapsed time = "+ lockElapsed);
		double delta = Math.abs( ((double)relockElapsed - (double)lockElapsed)/(double)lockElapsed);
		System.out.println("Delta = "+delta);
		assertEquals(.1, delta, .1);
	}

}
