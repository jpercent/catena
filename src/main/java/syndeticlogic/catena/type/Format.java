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
package syndeticlogic.catena.type;

public class Format {
   
    public static final int LONG_SIZE = 8;
    public static final int INT_SIZE = 4;
    public static final int SHORT_SIZE = 2;
	
    public static void packShort(byte[] dest, int offset, short src) {
        assert dest.length > offset && dest.length - offset >= SHORT_SIZE;
        for(int i=0, j=8; i < 2; i++, j -= 8) {
            dest[offset+i] = (byte)(src >>> j);
        }
    }
    
    public static void packInt(byte[] dest, int offset, int src) {
        assert dest.length > offset && dest.length - offset >= INT_SIZE;
        for(int i=0, j=24; i < 4; i++, j -= 8) {
            dest[offset+i] = (byte)(src >>> j);
        }
    }
    
    public static void packLong(byte[] dest, int offset, long src) {
        assert dest.length > offset && dest.length - offset >= LONG_SIZE;
        for(int i=0, j=56; i < 8; i++, j -= 8) {
            dest[offset+i] = (byte)(src >>> j);
        }
    }

    public static short unpackShort(byte[] src, int offset) {
        short ret=0;
        assert src.length > offset && src.length - offset >= 4;
        for(int i=0, j=8; i < 2; i++, j -= 8) {
            ret |= ((src[offset+i] & 0xff) << j);
        }
        return ret;
    }
    
    public static int unpackInt(byte[] src, int offset) {
        int ret=0;
        assert src.length > offset && src.length - offset >= 4;
        for(int i=0, j=24; i < 4; i++, j -= 8) {
            ret |= ((src[offset+i] & 0xff) << j);
        }
        return ret;
    }
    
    public static long unpackLong(byte[] src, int offset) {
        assert src.length > offset && src.length - offset >= 8;
        long ret=0;
        for(int i=0, j=56; i < 8; i++, j -= 8) {
            ret |= (long)(src[offset+i] & 0xff) << j;
        }
        return ret;
    }
}
