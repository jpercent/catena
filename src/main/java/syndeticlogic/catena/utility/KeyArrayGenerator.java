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
package syndeticlogic.catena.utility;

import java.io.FileOutputStream;
import java.io.File;
import java.util.ArrayList;

import syndeticlogic.catena.type.Codec;

public class KeyArrayGenerator {

	private File meta;
	private File data;
	private ArrayList<CompositeKey> keys;
	
    public KeyArrayGenerator(String baseName) throws Exception {
        meta = new File(baseName+".meta");
        data = new File(baseName+".data");
        keys = new ArrayList<CompositeKey>();
        FileOutputStream file = new FileOutputStream(meta);
        file.getChannel().truncate(0);
        file.flush();
        file.close();
        file = new FileOutputStream(data);
        file.getChannel().truncate(0);
        file.flush();
        file.close();
    }
    
    public void append(CompositeKey prefix, int numKeys) throws Exception {
        FileOutputStream metaOut = new FileOutputStream(meta, true);
        FileOutputStream dataOut = new FileOutputStream(data, true);
      
        Formatter f = new Formatter();
        
        for(int i = 0; i < numKeys; i++) {
            CompositeKey key = new CompositeKey();
            key.append(prefix);
            key.append(i);
            f.setInt(key.computeSize());
            metaOut.write(f.serialize());
            metaOut.flush();
        
            dataOut.write(CompositeKey.encode(key));
            dataOut.flush();
            keys.add(key);
        }
        
        
        metaOut.close();
        dataOut.close();
    }
    
    public ArrayList<CompositeKey> getKeys() {
        return keys;
    }
       
    private class Formatter {
        public Formatter() {
        	c = Codec.getCodec();
            intSet = false;
            value = 0;
        }
        
        public void setInt(int intval) {
            intSet = true;
            value = intval;
        }
        
        public byte[] serialize() {
            assert intSet == true;
            byte[] bytes = new byte[4];
            c.encode(value, bytes, 0);
            return bytes;
        }
        
        public Codec c;
        public boolean intSet;
        public int value;
    }
}
