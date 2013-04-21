package syndeticlogic.catena.text;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashSet;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class BasicTokenizer implements Tokenizer {
	private static final Log log = LogFactory.getLog(BasicTokenizer.class);
	
	public void tokenize(InvertedFileBuilder indexBuilder, File file, int docId) {
		FileInputStream inputStream=null;
		FileChannel channel=null;
		try {
			HashSet<String> document = new HashSet<String>();
			inputStream = new FileInputStream(file);
			channel = inputStream.getChannel();
			MappedByteBuffer buffer = channel.map(MapMode.READ_ONLY, 0L, channel.size());
			int blockSize = (int)Math.min(8192L, channel.size());
			byte[] block = new byte[blockSize];
			
			while(buffer.hasRemaining()) {
				if(blockSize != Math.min(buffer.remaining(), blockSize)) {
					blockSize = Math.min(buffer.remaining(), blockSize);
					block = new byte[blockSize];
				}
			    buffer.get(block, 0, blockSize); 
			    String str = new String(block, "UTF-8");
			    String[] words = str.split(" ");
			    for(String word : words) {
			    	if(!document.contains(word)) {
			    		document.add(word);
						indexBuilder.addWord(docId, word);
					}			    	
			    }
			}
		} catch(IOException e) {
			log.error("Could not tokenize "+file.getAbsolutePath()+": "+e, e);			
		} finally {
			try {
				inputStream.close();
				channel.close();
			} catch(Exception e) {}
		}
	}
}
