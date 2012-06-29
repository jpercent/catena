package org.syndeticlogic.cache;

public interface PinnableListener {
	void pin(Object key);
	void unpin(Object key);
}
