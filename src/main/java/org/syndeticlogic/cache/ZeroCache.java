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
 * Class ZeroCache
 * 
 * @author <a href="mailto:james@empty-set.net">James Percent</a>
 * @version $Revision: 1.0 $
 */
public class ZeroCache implements Cache {

	public final boolean addObject(Object userKey, Object cacheObject) {
		return false;
	}

	public final Object getObject(Object key) {
		return null;
	}

	public final int size() {
		return 0;
	}

	public final void remove(Object key) {
	}

	public final void clear() {
	}

	public final String toString() {
		return "ZeroCache";
	}

	@Override
	public void removeLeastValuableNode() {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeExpiredElements() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean addUncachedObject(Object userKey, Object cacheObject) {
		// TODO Auto-generated method stub
		return false;
	}
}
