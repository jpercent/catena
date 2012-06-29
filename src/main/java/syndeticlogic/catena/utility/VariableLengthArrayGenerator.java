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

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Random;

import syndeticlogic.catena.type.Format;

public class VariableLengthArrayGenerator implements ArrayGenerator {
    
    private static int MAX_ARRAY_SIZE = 65536;
	private File dataFile;
	private File metaFile;
	private Random random;
	private int seed;
    private int length;
    private long totalBytes;
    
    public VariableLengthArrayGenerator(int seed, int length) {
        this.seed = seed;
        this.length = length;
        dataFile = null;
        metaFile = null;
        random = null;
        totalBytes = 0;
    }
    
    public VariableLengthArrayGenerator(String baseName, int seed, int length) throws Exception {
    	assert Util.delete(baseName+".meta");
    	assert Util.delete(baseName+".data");
    	metaFile = new File(baseName+".meta");
        dataFile = new File(baseName+".data");
        random = null;
        this.seed = seed;
        this.length = length;
        this.totalBytes = 0;
        
    }
    
    public void generateFileArray() throws Exception {
	    assert metaFile != null && dataFile != null;
        
	    FileOutputStream metaOut = new FileOutputStream(metaFile);
	    FileOutputStream dataOut = new FileOutputStream(dataFile);
	    
	    metaOut.getChannel().truncate(0);
	    metaOut.flush();
	    dataOut.getChannel().truncate(0);
	    dataOut.flush();
	    
		Random localRandom = new Random(seed);
		
        for(int i = 0; i < length; i++) {
            int meta = 0;
            while(meta == 0) {
                meta = localRandom.nextInt(MAX_ARRAY_SIZE);
            }
            byte[] element = new byte[meta]; 
            localRandom.nextBytes(element);
		      /* System.out.println("generate file ===================================================================");		
				for(int k=0; k < element.length; k++) {
					System.out.print((Integer.toHexString((int)element[k] & 0x000000ff))+ ", ");
				}
		        System.out.println();*/                        
            byte[] length = new byte[4];
            Format.packInt(length, 0, meta);
            
            metaOut.write(length);
            metaOut.flush();

            dataOut.write(element);
            dataOut.flush();
		}
        metaOut.close();
        dataOut.close();
	}

	public ArrayList<byte[]> generateMemoryArray(int length) {
		
		if(random == null) {
			random = new Random(seed);
		}
		
		ArrayList<byte[]> elements = new ArrayList<byte[]>();
		for(int i = 0; i < length; i++) {
		      int meta = 0;
		      while(meta == 0) {
		          meta = random.nextInt(MAX_ARRAY_SIZE); 
		      }
		      
		      byte[] element = new byte[meta];
		      random.nextBytes(element);
		       /*System.out.println("generate memory ===================================================================");		
				for(int k=0; k < element.length; k++) {
					System.out.print((Integer.toHexString((int)element[k] & 0x000000ff))+ ", ");
				}
		        System.out.println();*/
		      elements.add(element);
		}
		return elements;
	}

	public long getTotalBytes() {
    	return totalBytes;
    }

	@Override
	public File getFile() {
		return dataFile;
	}

	@Override
	public void reset() {
		random = null;
	}
}