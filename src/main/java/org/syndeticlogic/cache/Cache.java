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

/**
 * Interface Cache
 * 
 * @author <a href="mailto:james@empty-set.net"> James Percent</a>
 * @version $Revision: 1.0 $
 */

public interface Cache {

	public interface CacheNode {
		boolean isExpired();
		Object getKey();
		void setKey(Object key);
		Object getValue();
		void setValue(Object value);
		String toString();
	}
	
	/**
	 * Adds an object to the cache if does not already reside in the cache.  If the object resides
	 * in the cache then revalue the object
	 * @return TODO
	 */
	boolean addObject(Object userKey, Object cacheObject);

	/**
	 * Adds an object that is known to not reside in the cache.
	 * @return TODO
	 */
	boolean addUncachedObject(Object userKey, Object cacheObject);


	/**
	 * adds an object to the cache
	 */
	//void addObject(Object obj);

	
	/**
	 * gets the value stored in the cache by it's key, or null if the key is not
	 * found.
	 */
	Object getObject(Object key);

	/**
	 * The number of key/value pares in the cache
	 */
	int size();

	/**
	 * remove a specific key/value pair from the cache
	 */
	void remove(Object key);

	/**
	 * Removes ALL keys and values from the cache. Use with digression. Using
	 * this method too frequently may defeat the purpose of caching.
	 */
	void clear();

	/**
	 * This method will execute the cache's eviction strategy. If this method is
	 * called, there will be at least one element in the cache to remove. The
	 * method itself does not need to check for the existance of a element.
	 * <p>
	 * This method is only called by shrinkToSize();
	 */
	void removeLeastValuableNode();

	/**
	 * Purge the cache of expired elements.
	 */
	void removeExpiredElements();

}
