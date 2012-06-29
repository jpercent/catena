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
import java.lang.ref.SoftReference;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * Class Reaper
 *
 * @author <a href="mailto:james@empty-set.net">James Percent</a>
 * @version $Revision: 1.0$
 */
class Reaper extends TimerTask {
	private static long instanceCounter = 0;
	private static final long instanceNumber = (instanceCounter++);
	private static final Log   LOG   = LogFactory.getLog(Reaper.class);
    private static final Timer TIMER = new Timer(true);
    Cache cache = null;

    public static Cache register(Cache cache, long period)
    {
        LOG.debug("register : " + cache);
        TIMER.scheduleAtFixedRate(new Reaper(cache), period, period);
        return cache;
    }
    
    private SoftReference<Cache> reference = null;
	private String toString;

	public Reaper(Cache cache) {
		this.toString = cache.toString();
		this.reference = new SoftReference<Cache>(cache);
	}

	public void run() {
		Cache reapableCache = (Cache) reference.get();
		String threadName = Thread.currentThread().getName();

		Thread.currentThread().setName("REAPER for " + instanceNumber);
		LOG.debug("run");

		if (reapableCache == null) {
			LOG.info("cache reaper quitting for : " + toString + " #"
					+ instanceNumber);
			cancel();
		} else {
			reapableCache.removeExpiredElements();
		}
		Thread.currentThread().setName(threadName);
	}

	
	/**
	 * Constructor Reaper
	 * 
	 * 
	 * @param cache
	 * @param key
	 * 	public Reaper(Cache cache) {
		this.cache = cache;
	}
	 */

	/**
	 * Method run
	 
	public void run() {
		String threadName = Thread.currentThread().getName();
		Thread.currentThread().setName("REAPER for " + instanceNumber);
		synchronized (cache) {
			cache.removeExpiredElements();
		}
		Thread.currentThread().setName(threadName);
	}
	*/
}
