package syndeticlogic.catena.text.io.old;

import static org.junit.Assert.*;

import java.io.File;
import java.util.TreeMap;

import org.codehaus.plexus.util.FileUtils;
import org.junit.Before;
import org.junit.Test;

import syndeticlogic.catena.text.BasicTokenizer;
import syndeticlogic.catena.text.CorpusManager;
import syndeticlogic.catena.text.postings.InvertedFileBuilder;
import syndeticlogic.catena.text.postings.InvertedList;
import syndeticlogic.catena.text.postings.Tokenizer;

public class RawInvertedFileReaderAndWriterTest {
	Tokenizer tokenizer;
	InvertedFileWriter fileWriter;
	InvertedFileReader fileReader;
	InvertedFileBuilder indexBuilder;
	CorpusManager corpusManager;
	String prefix;
	
	
	@Before
	public void setup() throws Exception {
    	prefix = "target"+File.separator+"corpus-manager"+File.separator;
    	FileUtils.deleteDirectory(prefix);
    	FileUtils.mkdir(prefix);
    	//fileWriter = new CatenaInvertedFileWriter();
    	fileWriter = new RawInvertedFileWriter();
    	tokenizer = new BasicTokenizer();
    	indexBuilder = new InvertedFileBuilder(prefix, fileWriter);
    	fileReader = new InvertedFileReader();
    	//tokenizer = new LuceneStandardTokenizer();
    	corpusManager = new CorpusManager(prefix, tokenizer, indexBuilder);
	}
	
	@Test
	public void testIndex() throws Throwable {
	   	
	   	String prefix = "src/test/resources/text/some/";
	   	File f = new File(prefix);
	   	System.out.println(f.getAbsolutePath());
	   	try {
	   	corpusManager.index("src/test/resources/text/some/");
	   	} catch(Throwable t) { t.printStackTrace(); }
	   	TreeMap<String, InvertedList> postings = new TreeMap<String, InvertedList>();
	   	fileReader.open("target/corpus-manager/corpus.index");
    	fileReader.scanFile(indexBuilder.getIdToWord(), postings);
    	assertEquals(indexBuilder.getPostings().size(), postings.size());
    	for(String key : indexBuilder.getPostings().keySet()) {
    		assertTrue(postings.containsKey(key));
    		assertTrue(indexBuilder.getPostings().get(key).deepCompare(postings.get(key)));
    	}
	}
}
