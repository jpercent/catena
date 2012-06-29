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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.syndeticlogic.cache.Cache.CacheNode;
import org.syndeticlogic.cache.PinnableListener;

/**
 * Class PinnableLruStrategy
 *
 * @author <a href="mailto:james@empty-set.net">James Percent</a>
 * @version $Revision: 1.0 $
 */
public class PinnableLruStrategy extends AbstractPolicyStrategy implements PinnableListener {
	
	private static final Log LOG = LogFactory.getLog(LruStrategy.class);
	private final PriorityQueue<PinnableNode> lru;
	private boolean defaultPinned;
	private boolean waiting;
	private int logicalClock;
	
	public static class PinnableNode extends BaseNode {		
		int time;
	}
	
	public class PinnableNodeComparator implements Comparator<PinnableNode> {
		@Override
		public int compare(PinnableNode arg0, PinnableNode arg1) {
			if(arg0.time < arg1.time)
				return -1;
			else if(arg0.time == arg1.time)
				return 0;
			else 
				assert arg0.time > arg1.time;
			
			return 1;				
		}	
	}

	public PinnableLruStrategy(EvictionListener elistener, int maxSize, long timeoutMillis) {
		super(elistener, maxSize, timeoutMillis);
		lru = new PriorityQueue<PinnableNode>(maxSize, new PinnableNodeComparator());
		defaultPinned = true;
		waiting = false;
	}
	
	public void setPinned() {
		defaultPinned = true;
	}
	
	public void setUnpinned() {
		defaultPinned = false;
	}
	
	@Override
	public CacheNode createNode(Object userKey, Object cacheObject) {
		if(map.size() == maxSize && lru.size() == 0) 
			return null;
		
		PinnableNode node = (PinnableNode) super.createNode(userKey, cacheObject);
		node.time = logicalClock++;
		if(logicalClock < 0) {
			assert false;
			revalueNodes();
		}
		//LOG.debug("logicalClock: "+logicalClock);
		if(!defaultPinned) {
			lru.add(node);
		}
		return node;
	}

	@Override
	public CacheNode createNode() {
		return new PinnableNode();
	}
	
	@Override
	public void revalueNode(CacheNode node) {
		assert node != null;
		((PinnableNode)node).time = logicalClock++;
		if(logicalClock < 0) {
			assert false;
			revalueNodes();
		} else if(lru.contains(node)) {
			lru.remove(node);
			lru.add((PinnableNode) node);
		}
	}
	
	@Override
	public void removeLeastValuableNode() {
		CacheNode node = null;
		node = lru.poll();
		if(node == null && map.size() > 0) {
			waiting = true;
		} else if(node != null) {
			delete(node);
		}
	}
	
	@Override
	public void delete(CacheNode node) {
		assert node != null;
		lru.remove(node);
		super.delete(node);
	}

	@Override
	public void pin(Object key) {
		PinnableNode node = (PinnableNode) map.get(key);
		assert node != null;
		assert lru.remove(node);
	}

	@Override
	public void unpin(Object key) {
		PinnableNode node = (PinnableNode) map.get(key);
		assert node != null;
		assert !lru.contains(node);
		if(waiting) {
			waiting = false;
			assert map.size() == 1;
			removeLeastValuableNode();
		} else {
			lru.add((PinnableNode) node);
		}
	}
	
	public String dumpPinnableLruKeys() {
		String dump = null;
		StringBuffer sb = new StringBuffer();
		ArrayList<PinnableNode> nodes = new ArrayList<PinnableNode>(lru.size());
		
		PinnableNode node = lru.poll();
		while (node != null) {
			sb.append(node.getKey());//+":"+node.time);
			nodes.add(node);
			LOG.debug(node.getKey()+":"+node.time);
			node = lru.poll();

		}
		dump = sb.toString();
		LOG.debug("dumpPinnableLruStrategy: " + dump);
		
		Iterator<PinnableNode> i = nodes.iterator();
		while(i.hasNext())
			lru.add(i.next());
		
		return dump;
	}
		
	private void revalueNodes() {
		assert false;
		ArrayList<PinnableNode> nodes = new ArrayList<PinnableNode>(lru.size());
		while(!lru.isEmpty()) {
			PinnableNode node = lru.poll();
			node.time = logicalClock++;
			nodes.add(lru.poll());
		}
		
		Iterator<PinnableNode> i = nodes.iterator();
		while(i.hasNext())
			lru.add(i.next());
	}
}
