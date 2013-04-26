package syndeticlogic.catena.text;

import static org.junit.Assert.*;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.codehaus.plexus.util.FileUtils;
import org.junit.Before;
import org.junit.Test;

import syndeticlogic.catena.text.io.BlockReader;
import syndeticlogic.catena.text.io.BlockWriter;
import syndeticlogic.catena.text.io.DictionaryReadCursor;
import syndeticlogic.catena.text.io.DictionaryWriteCursor;
import syndeticlogic.catena.text.io.InvertedFileReader;
import syndeticlogic.catena.text.io.InvertedFileWriter;
import syndeticlogic.catena.text.io.RawInvertedFileWriter;
import syndeticlogic.catena.text.io.ReadCursor;
import syndeticlogic.catena.text.io.WriteCursor;
import syndeticlogic.catena.text.postings.InvertedFileBuilder;
import syndeticlogic.catena.text.postings.InvertedList;
import syndeticlogic.catena.text.postings.InvertedListDescriptor;
import syndeticlogic.catena.text.postings.Tokenizer;

public class BlockReaderWriterTest {
	private BlockReader reader;
	private BlockWriter writer;
    
	public class TestWriteCursor implements WriteCursor {
	    int headerLen = 5;
	    long fileOffset = 0;
	    int hasNextCount = 3;
        private int hasNext = 1048576;
        private int blockSize;
        private byte[] buffer;
        private int offset;

	    
        @Override
        public int reserveHeaderLength() {
            return headerLen;
        }

        @Override
        public void setFileOffset(long fileOffset) {
            this.fileOffset = fileOffset;
        }

        @Override
        public boolean hasNext() {
            if(hasNext > 0) {
                hasNext --;
                return true;
            }
            return false;
        }

        @Override
        public int nextLength() {
            // TODO Auto-generated method stub
            return blockSize;
        }

        @Override
        public int encodeNext(byte[] buffer, int offset) {
            this.buffer = buffer;
            this.offset = offset;
            return buffer.length;
        }

        @Override
        public int encodeHeader(byte[] buffer, int offset) {
            // TODO Auto-generated method stub
            return 0;
        }
	    
	}
	
	@Before
	public void setup() throws Exception {
	    String fileName = "target/block-reader-writer-test";
	    reader = new BlockReader();
	    writer = new BlockWriter();
	    Map<Integer, String> idDocMap = new HashMap<Integer, String>();
        List<InvertedListDescriptor> descriptors = new LinkedList<InvertedListDescriptor>();
	    //Random r = new Random(1337);
	    for(int i = 0; i < 1048576; i++) {
	        String word = Integer.toString(i);
	        idDocMap.put(i, word);
	        descriptors.add(new InvertedListDescriptor(word, i, word.length(), i));
	    }
	    
	    WriteCursor writeCursor = new DictionaryWriteCursor(null, null);
	    writer.open(fileName);
	    writer.writeBlock(writeCursor);
	
	    Map<Integer, String> idDocMap1 = new HashMap<Integer, String>();
        List<InvertedListDescriptor> descriptors1 = new LinkedList<InvertedListDescriptor>();
	    ReadCursor readCursor = new DictionaryReadCursor(idDocMap, descriptors);
	    reader.open(fileName);
	    reader.read(readCursor);
	    
	    assertEquals(idDocMap.size(), idDocMap.size());
	    assertEquals(descriptors.size(), descriptors1.size());
	    
	    for(int i = 0; i < 1048576; i++) {
	        assertEquals(idDocMap.get(i), idDocMap1.get(i));
	        assertEquals(descriptors.get(i), descriptors1.get(i));
	    }
	}
}
