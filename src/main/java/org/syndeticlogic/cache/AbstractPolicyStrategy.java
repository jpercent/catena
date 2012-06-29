package org.syndeticlogic.cache;
/*
 * Author == James Percent (james@empty-set.net) 
 *
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

import syndeticlogic.catena.utility.Util;

import java.util.Map;

/**
 * Class LruStrategy
 * 
 * @author <a href="mailto:james@empty-set.net">James Percent</a>
 * @version $Revision: 1.0 $
 */
public abstract class AbstractPolicyStrategy implements PolicyStrategy {
	
	private static final Log LOG = LogFactory.getLog(AbstractPolicyStrategy.class);
	protected final Map<Object, Object> map;
	protected final LinkedList fifo;
	protected final int maxSize;
	protected final long timeoutMillis;
	protected EvictionListener elistener;
	
	public static class BaseNode implements Cache.CacheNode {
		Object key;
		Object value;	
		LinkedListNode fifoNode;
		long timeoutTime = 0;
		
		public final boolean isExpired() {
			long timeToGo = timeoutTime - System.currentTimeMillis();
			return (timeToGo <= 0);
		}
		
		public final Object getKey() {
			return key;
		}
		
		public final void setKey(Object key) {
			this.key = key;
		}

		public final Object getValue() {
			return this.value;
		}

		public final void setValue(Object value) {
			this.value = value;
		}
	}
	
	public AbstractPolicyStrategy(EvictionListener elistener, int maxSize, long timeoutMillis) {
		this.elistener = elistener;
		this.maxSize = maxSize;
		this.timeoutMillis = timeoutMillis;
		map = Util.createMap(maxSize);
		fifo = new LinkedList();
	}
	
	@Override
	public void setEvictionListener(EvictionListener elistener) {
		this.elistener = elistener;
	}
	
	@Override
	public Cache.CacheNode findNodeByKey(Object key) {
		return (Cache.CacheNode) map.get(key);
	}
	
	@Override
	public void delete(Cache.CacheNode node) {
		assert node != null && node instanceof BaseNode;
		fifo.remove(((BaseNode)node).fifoNode);
		map.remove(node.getKey());
		if(elistener != null)
			elistener.evicted(node.getKey(), node.getValue());
		node = null;
	}

	@Override
	public void removeLeastValuableNode() {
		LinkedListNode lln = null;
		Cache.CacheNode node = null;
		lln = fifo.peekLast();
		if(lln != null) {
			node = (Cache.CacheNode) lln.getValue();
			if(node != null) {
				delete(node);
				node = null;
			}
		}
	}
	
	@Override
	public void removeExpiredElements() {
		LinkedListNode lln = null;
		Cache.CacheNode node = null;
		while ((lln = fifo.peekLast()) != null) {
			node = (Cache.CacheNode) lln.getValue();
			if (node != null && node.isExpired()) {
				delete(node);
				node = null;
			} else {
				// not expired.. can stop now
				break;
			}
		}
	}
	
	@Override
	public Cache.CacheNode createNode(Object userKey, Object cacheObject) {
		assert size() <= maxSize;
		BaseNode node = (BaseNode) createNode();
		if(size() == maxSize)
			removeLeastValuableNode();
		
		node.setKey(userKey);
		node.setValue(cacheObject);
		node.fifoNode = fifo.addFirst(node);
		node.timeoutTime = System.currentTimeMillis()+timeoutMillis;
		map.put(userKey, node);
		return node;
	}

	@Override
	public long getTimeoutMilliSeconds() {
		return timeoutMillis;
	}

	@Override
	public int getMaxSize() {
		return maxSize;
	}
	
	@Override
	public int size() {
		return map.size();
	}
	
	public String dumpFifoKeys() {
		return dumpKeys("dumpFifoKeys: ", fifo);
	}
	
	protected String dumpKeys(String message, LinkedList list) {
		String dump = null;
		StringBuffer sb = new StringBuffer();
		LinkedListNode node = list.peekFirst();
		Cache.CacheNode current = null;

		while (node != null) {
			current = (Cache.CacheNode) node.getValue();
			sb.append(current.getKey());
			node = node.getNext();
		}

		dump = sb.toString();
		LOG.debug(message + dump);
		return dump;
	}
}
