package syndeticlogic.catena.text;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

import syndeticlogic.catena.text.io.BlockReader;
import syndeticlogic.catena.text.io.BlockWriter;
import syndeticlogic.catena.text.io.DictionaryReadCursor;
import syndeticlogic.catena.text.io.DictionaryWriteCursor;
import syndeticlogic.catena.text.io.InvertedFileReadCursor;
import syndeticlogic.catena.text.io.InvertedFileWriteCursor;
import syndeticlogic.catena.text.io.ReadCursor;
import syndeticlogic.catena.text.io.WriteCursor;
import syndeticlogic.catena.text.postings.InvertedList;
import syndeticlogic.catena.text.postings.InvertedListDescriptor;

public class BlockReaderWriterTest {
	private BlockReader reader;
	private BlockWriter writer;

	@Before
	public void setup() {
        reader = new BlockReader();
        writer = new BlockWriter();
	}

    @Test
    public void testInvertedFileReadWrite() throws Exception {
        doInverteFile(10, 17);
        //doInverteFile(1048576, 1048576);
        doInverteFile(1024, 1);
        doInverteFile(21337, 1048575);
        doInverteFile(3133, 8191);
        doInverteFile(3133, 8192);
        doInverteFile(3133, 8198);
    }
        
    public void doInverteFile(int items, int blockSize) throws Exception {
        String fileName = "target/block-reader-writer-test-invertedFile";

///     int items = 10;
        Map<Integer, String> idDocMap = new HashMap<Integer, String>();
        TreeMap<String, InvertedList> invertedList = new TreeMap<String, InvertedList>();
        //Random r = new Random(1337);
        for(int i = 0; i < items; i++) {
            
            String word = Integer.toString(i)+"/"+Integer.toString(i);
            idDocMap.put(i, word);
            InvertedList list = InvertedList.create();
            list.setWordId(i);
            invertedList.put(word, list);
            Random rand = new Random(11);
            int mod = rand.nextInt() % items;
            list.setWord(word);
            for(int j = 0; j < items; j++) {
                if(j % mod == 0) {
                    list.addDocumentId(j);
                }
                
            }
        }
        
        WriteCursor writeCursor = new InvertedFileWriteCursor(invertedList);
        writer.open(fileName);
        writer.writeFile(writeCursor);
        writer.close();
        
        TreeMap<String, InvertedList> invertedList1 = new TreeMap<String, InvertedList>(); 
        InvertedFileReadCursor readCursor = new InvertedFileReadCursor(idDocMap);
        
        reader.open(fileName);
        reader.setBlockSize(items+17);
        reader.read(readCursor);
        reader.close();
        invertedList1 = readCursor.getInvertedList();
        assertEquals(invertedList.size(), invertedList1.size());
        assertTrue(invertedList.equals(invertedList1));
    }
	
    @Test
    public void testDictionaryReadWrite() throws Exception {
        doDictionary(10, 17);
        doDictionary(1048576, 1048576);
        doDictionary(1024, 1);
        doDictionary(31337, 8191);
    }
        
    public void doDictionary(int items, int blockSize) throws Exception {
        String fileName = "target/block-reader-writer-test-dictionary";

///     int items = 10;
        Map<Integer, String> idDocMap = new HashMap<Integer, String>();
        List<InvertedListDescriptor> descriptors = new LinkedList<InvertedListDescriptor>();
        //Random r = new Random(1337);
        for(int i = 0; i < items; i++) {
            String word = Integer.toString(i)+"/"+Integer.toString(i);
            idDocMap.put(i, word);
            descriptors.add(new InvertedListDescriptor(word, i, word.length(), i));
        }
        
        WriteCursor writeCursor = new DictionaryWriteCursor(idDocMap, descriptors);
        writer.open(fileName);
        writer.writeFile(writeCursor);
        writer.close();
        
        Map<Integer, String> idDocMap1 = new HashMap<Integer, String>();
        List<InvertedListDescriptor> descriptors1 = new LinkedList<InvertedListDescriptor>();
        ReadCursor readCursor = new DictionaryReadCursor(idDocMap1, descriptors1);
        reader.open(fileName);
        reader.setBlockSize(items+17);
        reader.read(readCursor);
        reader.close();
        assertEquals(idDocMap.size(), idDocMap.size());
        assertEquals(descriptors.size(), descriptors1.size());
        assertEquals(idDocMap, idDocMap1);
        assertTrue(idDocMap.equals(idDocMap1));
        assertTrue(descriptors.equals(descriptors1));
    }
}
