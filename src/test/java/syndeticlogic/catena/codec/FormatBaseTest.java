/*
 *    Author: James Percent (james@empty-set.net)
 *    Copyright 2010, 2011 James Percent
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * 
 */
package syndeticlogic.catena.codec;

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import syndeticlogic.catena.utility.Format;

public class FormatBaseTest {
	private static final Log log = LogFactory.getLog(FormatBaseTest.class);
	
    @Test
    public void testPackUnpackInt() {
        int a = 0xdeadbeef;
        int b = 0;
        byte[] c = new byte[Format.INT_SIZE+1];
        
        Format.packInt(c, 1, a);
        b = Format.unpackInt(c, 1);
        log.debug("a = "+ Integer.toHexString(a) + ", b = "+Integer.toHexString(b));
        assertEquals(a,b);
    }

    @Test
    public void testPackUnpackLong() {
        long a = 0xdeadbeefdeadbeefL;
        long b = 0;
        byte[] c = new byte[Format.LONG_SIZE+31];
        Format.packLong(c, 10, a);
        b = Format.unpackLong(c, 10);
        assertEquals(a,b);        
    }
}
