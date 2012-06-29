package org.syndeticlogic.cache;

public interface PolicyStrategy {	
	void setEvictionListener(EvictionListener elistener);
	Cache.CacheNode createNode();
	Cache.CacheNode createNode(Object userKey, Object cacheObject);
	Cache.CacheNode findNodeByKey(Object key);
	void revalueNode(Cache.CacheNode node);
	void removeLeastValuableNode();
	void removeExpiredElements();
	void delete(Cache.CacheNode node);
	long getTimeoutMilliSeconds();
	int getMaxSize();
	int size();
}
