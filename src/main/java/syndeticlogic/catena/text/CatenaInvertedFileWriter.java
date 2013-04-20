package syndeticlogic.catena.text;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.SortedMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.array.Array;
import syndeticlogic.catena.type.CodeableValue;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;

public class CatenaInvertedFileWriter implements InvertedFileWriter {
	private static final Log log = LogFactory.getLog(RawInvertedFileWriter.class);
	public long writeFile(String name, SortedMap<String, InvertedList> postings, Map<Integer, Long> wordToOffset) {
		long fileOffset=0;
		try {
	        Array<CodeableValue> index = (Array<CodeableValue>) Array.create(name, Type.CODEABLE);
	        CodeableValue value = new CodeableValue();
	        System.out.println("Appending... "+postings.values().size());
	        int count = 0;
	        long start = System.currentTimeMillis();
	        
	        byte[] jvm = new byte[1048576];
			int offset=0;
	        
	        for(InvertedList list : postings.values()) {
	            //System.out.println(postingsList.getWord());
	        	int length = Type.CODEABLE.length() + list.size();
	        	if(offset+length >= 1048576) {
	                index.appendBulk(jvm, offset, jvm.length - offset);
	                index.commit();
	        		offset = 0;
	        	}
	        	//wordToOffset.put(list.getWordId(), fileOffset);
	        	int written = Codec.getCodec().encode(list, jvm, offset);
	        	assert written == length;
	        	offset += length;
	        	fileOffset += length;
	        	
	        	//value.reset(postingsList);

	        	count ++;            	
	        	}
	    	index.appendBulk(jvm, offset, jvm.length - offset);
	    	index.commit();
			System.out.println("Records "+count+" elapsed seconds = "+(System.currentTimeMillis()-start));

	        System.out.println("done appending... ");
	        index.commit();    		
		} catch(Throwable e) {
			log.fatal("could not create index: "+e, e);
		
		}finally {
			
		}
		return fileOffset;
	}
}
