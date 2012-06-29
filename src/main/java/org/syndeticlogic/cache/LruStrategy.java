package org.syndeticlogic.cache;
/*
 * Copyright 2010, 2011 James Percent
 * 
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 */
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.utility.LinkedList;
import syndeticlogic.catena.utility.LinkedListNode;

/**
 * Class LruStrategy
 * 
 * @author <a href="mailto:james@empty-set.net">James Percent</a>
 * @version $Revision: 1.0 $
 */
public class LruStrategy extends AbstractPolicyStrategy {
	
	private static final Log log = LogFactory.getLog(LruStrategy.class);
	private final LinkedList lru;
	
	public static class LruNode extends BaseNode {
		private LinkedListNode lruNode;
		
	}
	public LruStrategy(EvictionListener elistener, int maxSize, long timeoutMillis) {
		super(elistener, maxSize, timeoutMillis);
		lru = new LinkedList();
		if(log.isTraceEnabled()) log.trace("LruStrategy creaed; size = "+maxSize+" duration = "+timeoutMillis);
	}
	
	@Override
	public void revalueNode(Cache.CacheNode node) {
		assert node != null && node instanceof LruNode;
		lru.moveToFirst(((LruNode)node).lruNode);
	}

	@Override
	public void delete(Cache.CacheNode node) {
		assert node != null && node instanceof LruNode;
		lru.remove(((LruNode)node).lruNode);
		super.delete(node);
		node = null;
	}

	@Override
	public void removeLeastValuableNode() {
		LinkedListNode lln = null;
		Cache.CacheNode node = null;
		lln = lru.peekLast();
		if(lln != null) {
			node = (Cache.CacheNode) lln.getValue();
			delete(node);
		}
	}
		
	@Override
	public Cache.CacheNode createNode(Object userKey, Object cacheObject) {
		LruNode node = (LruNode) super.createNode(userKey, cacheObject);
		node.lruNode = lru.addFirst(node);
		return (Cache.CacheNode) node;
	}
	
	public Cache.CacheNode createNode() {
		return (Cache.CacheNode) new LruNode();
	}
	
	public String dumpLruKeys() {
		return super.dumpKeys("dumpLruKeys: ", lru);
	}
}
