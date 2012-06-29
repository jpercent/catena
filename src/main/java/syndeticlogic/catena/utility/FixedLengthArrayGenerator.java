/*
 *    Author == James Percent (james@empty-set.net)
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

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import syndeticlogic.catena.zold.arrays.FixedLengthArray;

public class FixedLengthArrayGenerator implements ArrayGenerator {
	private static final Log log = LogFactory.getLog(FixedLengthArrayGenerator.class);
	private File file;
	private Random random;
	private int seed;
	private String filename;
	private int elementSize;
	private int elementLength;
	
    public FixedLengthArrayGenerator(String fileName, int seed, int elementSize, int length) throws Exception {
        this.seed = seed;
        this.elementSize = elementSize;
        this.elementLength = length;
        random = null;
        filename = fileName;
        assert Util.delete(filename);
    }
    
    public FixedLengthArrayGenerator(int seed, int fieldSize, int numFields) {
        this.seed = seed;
        this.elementSize = fieldSize;
        this.elementLength = numFields;
        file = null;
        random = null;
    }
        
    public void generateFileArray() throws Exception {
        file = new File(filename);
        FileOutputStream out = new FileOutputStream(file);
        out.getChannel().truncate(0);
        out.flush();

        Random localRandom = new Random(seed);
    

        for(int i = 0; i < elementLength; i++) {
            byte [] buffer = new byte[elementSize];
            localRandom.nextBytes(buffer);
            out.write(buffer);
            out.flush();
            if(log.isTraceEnabled()) {
            System.out.println("generate file "+i+" ===================================================================");      
            for(int k=0; k < buffer.length; k++) {
                System.out.print((Integer.toHexString((int)buffer[k] & 0xff))+ ", ");
            }
            System.out.println();
            }
        }
        out.close();
	}

	public ArrayList<byte[]> generateMemoryArray(int elements) {
		
		if(random == null) {
			random = new Random(seed);
		}

		ArrayList<byte[]> array = new ArrayList<byte[]>();
		byte [] element = new byte[elementSize];
		for(int i = 0; i < elements; i++) {
			random.nextBytes(element);
		     /*  System.out.println("generate memory ===================================================================");		
				for(int k=0; k < element.length; k++) {
					System.out.print((Integer.toHexString((int)element[k] & 0x000000ff))+ ", ");
				}
		        System.out.println();
		        */
		        array.add(element);
	      
			
			element = new byte[elementSize];
		
		}
		return array;
	}
	
	public File getFile() {
		return file;
	}
	
	public void reset() {
		random = null;
	}
	
	public static void main(String [] args) throws Exception {
		FixedLengthArrayGenerator one = new FixedLengthArrayGenerator("file0", 1337, 821, 226);
		FixedLengthArrayGenerator two = new FixedLengthArrayGenerator("file1", 1337, 821, 226);
		try { 
			one.generateFileArray();
			two.generateFileArray();
		} catch(Exception e) {
			log.error(e.toString());
		}
	}
}