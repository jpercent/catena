package syndeticlogic.catena.text;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.SortedMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;

public class RawInvertedFileWriter implements InvertedFileWriter {
	private static final Log log = LogFactory.getLog(RawInvertedFileWriter.class);
	public long writeFile(String name, SortedMap<String, InvertedList> postings, Map<Integer, Long> wordToOffset) {
		FileOutputStream outputStream=null;
		FileChannel channel=null;
		long fileOffset = 0;
		try {
			int PAGE_SIZE = 1048576/2/2;
			System.out.println("Creating file"+PAGE_SIZE);
			//File indexFile = new File("/home/james/catena/corpus.index");
			File indexFile = new File(name);
			indexFile.delete();
			assert indexFile.createNewFile();
			outputStream = new FileOutputStream(indexFile);
			channel = outputStream.getChannel();
			ByteBuffer direct = ByteBuffer.allocateDirect(PAGE_SIZE);
			byte[] jvm = new byte[PAGE_SIZE];
			int offset=0;

			System.out.println("Looping through list");
			int count = 0;
			long start = System.currentTimeMillis();
	        for(InvertedList list : postings.values()) {
	        	int length = Type.CODEABLE.length() + list.size();
	        	if(offset+length >= PAGE_SIZE) {
	        		direct.put(jvm, 0, offset);
	        		direct.rewind();
	        		direct.limit(offset);
	        		channel.write(direct);
	        		direct.rewind();
	        		direct.limit(direct.capacity());
	        		offset = 0;
	        	}
	        	wordToOffset.put(list.getWordId(), fileOffset);
	        	int written = Codec.getCodec().encode(list, jvm, offset);
	        	assert written == length;
	        	offset += length;
	        	fileOffset += length;
	        	count++;
	        }
    		direct.put(jvm, 0, offset);
    		
    		direct.rewind();
    		direct.limit(offset);
    		channel.write(direct);
    		direct.rewind();
    		direct.limit(direct.capacity());

			System.out.println("Records "+count+" elapsed seconds = "+(System.currentTimeMillis()-start));
	        System.out.println("Done totalData = "+fileOffset);
    		channel.force(true);
    		channel.close();
    		outputStream.close();
    		
		} catch(Throwable e) {
			log.fatal("could not create index: "+e, e);
		
		}finally {
			
		}
		return fileOffset;
	}
}
