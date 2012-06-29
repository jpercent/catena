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

import syndeticlogic.catena.utility.Util;
/**
 * Class PolicyCache
 * 
 * @author <a href="mailto:james@empty-set.net">James Percent</a>
 * @version $Revision: 1.0 $
 */
public class PolicyCache implements Cache {
	
	private static final Log log = LogFactory.getLog(PolicyCache.class);
	private final PolicyStrategy pstrategy;
	private final String name;
	
	public PolicyCache(PolicyStrategy ps, String name) {
		this.pstrategy = ps;
		this.name = name;
		if(log.isTraceEnabled()) log.trace(name+ " PolicyCache creaed");
	}
	
	@Override
	public final boolean addObject(Object userKey, Object cacheObject) {
		Cache.CacheNode node;
		node = pstrategy.findNodeByKey(userKey);
		boolean ret = true;

		if (node != null) {
			// if the node exists, then set it's value, and revalue it.
			// this is better than deleting it, because it doesn't require
			// more memory to be allocated
			node.setValue(cacheObject);
			pstrategy.revalueNode(node);
		} else {
			node = pstrategy.createNode(userKey, cacheObject);
			if(node == null)
				ret = false;
		}
		pstrategy.removeExpiredElements();
		return ret;
	}

	@Override
	public final boolean addUncachedObject(Object userKey, Object cacheObject) {
		Cache.CacheNode node;
		// XXX - turn this off for performance
		//assert pstrategy.findNodeByKey(userKey) == null;
		boolean ret = true;
		node = pstrategy.createNode(userKey, cacheObject);
		if(node == null)
			ret = false;

		pstrategy.removeExpiredElements();
		return ret;
	}

	@Override
	public final Object getObject(Object key) {
		Object value = null;
		Cache.CacheNode node;
		
		removeExpiredElements();
		node = pstrategy.findNodeByKey(key);
		if (node == null) {
			; // cache miss
		} else if (node.isExpired()) {
			pstrategy.delete(node);
			node = null;
		} else if (node != null) {
			pstrategy.revalueNode(node);
			value = node.getValue();
		}
		return value;
	}
	
	@Override
	public final void remove(Object key) {
		Cache.CacheNode node = pstrategy.findNodeByKey(key);
		if (node != null) {
			pstrategy.delete(node);
	        node = null;			
		}
		removeExpiredElements();
	}

	/**
	 * This method calls shrinkToSize(0) which will loop through each element,
	 * removing them one by one (in order of lease valued to most valued).
	 * Derived classes may wish to implement this in a more efficient way (by
	 * just reinitalizing itself).
	 */
	@Override
	public void clear() {
		while(size() > 0)
			removeLeastValuableNode();
	}
	
	public String toString() {
		return Util.shortName(getClass()) + "(" + name + ","
				+ pstrategy.getTimeoutMilliSeconds() + "," + pstrategy.getMaxSize() + ")";
	}
	
	@Override
	public int size() {
		return pstrategy.size();
	}

	@Override
	public void removeLeastValuableNode() {
		pstrategy.removeLeastValuableNode();
	}

	@Override
	public void removeExpiredElements() {
		pstrategy.removeExpiredElements();	
	}
}
