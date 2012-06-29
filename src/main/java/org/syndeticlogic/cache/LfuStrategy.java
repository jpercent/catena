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

import java.util.ArrayList;
import java.util.List;
/**
 * Class LfuStrategy
 * 
 * @author <a href="mailto:james@empty-set.net">James Percent</a>  
 * @version $Revision: 1.0 $
 */
public class LfuStrategy extends AbstractPolicyStrategy {

	private static final Log LOG = LogFactory.getLog(LfuStrategy.class);
	private final List<Object> lrus;
	private int maxLruBuckets = 0;

	// when searching for a node to remove, the lowest lru bucked is checked
	// then then next, etc etc. In some rare cases, we have extra information
	// that
	// would allow a higher bucket to be used to start the search.
	// This is a minor optimizaton.
	private int lowestNonEmptyLru = 0;

	public static class LfuNode extends BaseNode {
		LinkedListNode lfuNode;
		int numUsages;
	}
	
	public LfuStrategy(EvictionListener listener, int maxSize, long timeoutMilliSeconds) {
		super(listener, maxSize, timeoutMilliSeconds);
		lrus = new ArrayList<Object>(5);
		maxLruBuckets = maxSize * 3;
	}

	@Override
    public void removeLeastValuableNode() {
        LinkedList lfu = getLowestNonEmptyLru();
        LinkedListNode lln = lfu.peekLast();
        if(lln != null) {
        	Cache.CacheNode node = (Cache.CacheNode) lln.getValue();
        	delete(node);
        	node = null;
        }
	}	
	
	@Override
	public void revalueNode(Cache.CacheNode node) {
		assert node != null && node instanceof LfuNode;
		LfuNode n = (LfuNode)node;
		
		LinkedListNode lln = n.lfuNode;
		LinkedList currBucket = lru(n.numUsages);
		LinkedList nextBucket = lru(++n.numUsages);
		currBucket.remove(lln);
		n.lfuNode = nextBucket.addFirst(lln.getValue());
	}
	
	@Override
	public void delete(Cache.CacheNode node) {
		assert node != null && node instanceof LfuNode;
		LfuNode n = (LfuNode)node;
		lru(n.numUsages).remove(n.lfuNode);
		super.delete(n);
		node = null;
	}
	
	@Override
	public Cache.CacheNode createNode(Object userKey, Object cacheObject) {
		LfuNode node = (LfuNode)super.createNode(userKey, cacheObject);
		node.lfuNode = lru(0).addFirst(node);
		lowestNonEmptyLru = 0;
		return node;
	}

	public String dumpLfuKeys() {
		String dump = null;
		StringBuffer sb = new StringBuffer();
		LinkedListNode node = null; // lfu.peekFirst();
		Cache.CacheNode current = null;
		for (int i = lrus.size() - 1; i >= 0; i--) {
			node = lru(i).peekFirst();
			while (node != null) {
				current = (Cache.CacheNode) node.getValue();
				sb.append(current.getKey());
				node = node.getNext();
			}
		}

		dump = sb.toString();
		LOG.debug("dumpLfuKeys : " + dump);
		return dump;
	}

	protected LinkedList getLowestNonEmptyLru() {
		LinkedList lru = null;
		for (int i = lowestNonEmptyLru; i < lrus.size(); i++) {
			lru = lru(i);
			if (lru.size() != 0) {
				lowestNonEmptyLru = i;
				return lru;
			}
		}
		return lru;
	}

	protected final LinkedList lru(int numUsageIndex) {
		LinkedList lru = null;
		int lruIndex = Math.min(maxLruBuckets, numUsageIndex);

		if (lruIndex >= lrus.size()) {
			lru = new LinkedList();
			lrus.add(lruIndex, lru);
		} else {
			lru = (LinkedList) lrus.get(lruIndex);
		}
		return lru;
	}

	@Override
	public Cache.CacheNode createNode() {
		return new LfuNode();
	}
}
