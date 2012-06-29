package org.syndeticlogic.cache;

public interface Pinnable {
	void pin(); 	
	void unpin();
	boolean isPinned(); 
}
