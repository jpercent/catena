package syndeticlogic.catena.text.io.old;

import static org.junit.Assert.*;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.codehaus.plexus.util.FileUtils;
import org.junit.Test;

import syndeticlogic.catena.text.BasicTokenizer;
import syndeticlogic.catena.text.CorpusManager;
import syndeticlogic.catena.text.postings.IdTable;
import syndeticlogic.catena.text.postings.InvertedFileBuilder;
import syndeticlogic.catena.text.postings.InvertedList;
import syndeticlogic.catena.text.postings.InvertedListDescriptor;
import syndeticlogic.catena.text.postings.Tokenizer;

public class DictionaryReaderWriterTest {
	Tokenizer tokenizer;
	InvertedFileWriter fileWriter;
	InvertedFileReader fileReader;
	InvertedFileBuilder indexBuilder;
	CorpusManager corpusManager;
	String prefix;


	public void createIndex(String source) {
	    File f = new File(prefix);
        corpusManager.index(source);
	}
	
	public void createFromSingleBlock() {
	}
	
	@Test
	public void testIndex() throws Throwable {
        InvertedList.setTableType(IdTable.TableType.VariableByteCoded);
        test();
	    
	}
	
	@Test
	public void testIndexUncoded() throws Throwable {
        InvertedList.setTableType(IdTable.TableType.Uncoded);
        test();
	}
	
	public void test() throws Throwable {
        String base = "target/directory-read-write-test";
        prefix = base+File.separator;
        FileUtils.deleteDirectory(prefix);
        FileUtils.mkdir(prefix);
        //fileWriter = new CatenaInvertedFileWriter();
        fileWriter = new RawInvertedFileWriter();
        tokenizer = new BasicTokenizer();
        InvertedFileBuilder indexBuilder = new InvertedFileBuilder(prefix, fileWriter);
        fileReader = new InvertedFileReader();
        //tokenizer = new LuceneStandardTokenizer();
        corpusManager = new CorpusManager(prefix, tokenizer, indexBuilder);
        	    
	    createIndex("src/test/resources/text/split/");
	    
	    Map<Integer, String> idToDoc = indexBuilder.getIdToDoc();
        List<InvertedListDescriptor> desc = indexBuilder.getInvertedListDescriptors();
        
        Map<Integer, String> idToDoc1 = new HashMap<Integer, String>();
        List<InvertedListDescriptor> desc1 = new LinkedList<InvertedListDescriptor>();
        
        
        DictionaryReader dictionary = new DictionaryReader();
        dictionary.open(prefix+File.separator+"index.meta");
        dictionary.loadDictionary(idToDoc1, desc1);
        dictionary.close();
        assertEquals(idToDoc.size(), idToDoc1.size());
        assertEquals(desc.size(), desc1.size());
        
        for(Integer key : idToDoc.keySet()) {
            assertEquals(idToDoc.get(key), idToDoc1.get(key));
        }

        for(int i =0; i<desc.size(); i++) {
            assertTrue(desc.get(i).equals(desc1.get(i)));
        }
        /*for(InvertedListDescriptor desc : descs) {
            assertEquals(idToDoc.get(key), idToDoc.get(key));
        }*/

	    fileWriter.close();
	    fileWriter = null;
	    tokenizer = null;
	    indexBuilder = null;
	    if(fileReader != null) fileReader.close();
	    fileReader = null;
	    corpusManager = null;
	}
}
