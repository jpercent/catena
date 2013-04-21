package syndeticlogic.catena.text;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.TreeMap;

import org.codehaus.plexus.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BlockMergerTest {
	Tokenizer tokenizer;
	InvertedFileWriter fileWriter;
	InvertedFileReader fileReader;
	InvertedFileBuilder indexBuilder;
	CorpusManager corpusManager;
	String prefix;
	
	@Before
    public void setup() throws Exception {
        prefix = "target"+File.separator+"corpus-manager-block-merger-test"+File.separator;
        FileUtils.deleteDirectory(prefix);
        FileUtils.mkdir(prefix);
        fileWriter = new RawInvertedFileWriter();
        tokenizer = new BasicTokenizer();
        indexBuilder = new InvertedFileBuilder(prefix, "corpus.index", fileWriter);
        fileReader = new InvertedFileReader();
        corpusManager = new CorpusManager(prefix, tokenizer, indexBuilder);
    }
	
	@After 
	public void teardown() throws Exception {
        fileWriter.close();
        fileWriter = null;
        tokenizer = null;
        indexBuilder = null;
        if(fileReader != null) fileReader.close();
        fileReader = null;
        corpusManager = null;
	}

	public void createIndex(String source) {
	    File f = new File(prefix);
	    System.out.println(f.getAbsolutePath());
	    try {
	        corpusManager.index(source);
	    } catch(Throwable t) { t.printStackTrace(); }
	}
	
	public void createFromSingleBlock() {
	}
	
	public void compare(InvertedFileBuilder indexBuilder, InvertedFileBuilder indexBuilder1) throws IOException {
        
        TreeMap<String, InvertedList> postings = new TreeMap<String, InvertedList>();
        fileReader.open("target/corpus-manager-block-merger-test/merged/intermediate-merge-target.0");
        fileReader.scanFile(indexBuilder.getIdToWord(), postings);
        fileReader.close();

        TreeMap<String, InvertedList> postings1 = new TreeMap<String, InvertedList>();
        fileReader.open("target/corpus-manager-block-merger-test/single/depth1-0.corpus");
        fileReader.scanFile(indexBuilder1.getIdToWord(), postings1);
        fileReader.close();
        
        assertEquals(postings.size(), postings1.size());

        for(String key : postings.keySet()) {
            assertTrue(postings1.containsKey(key));
            assertEquals(postings1.get(key).getDocumentFrequency(), postings.get(key).getDocumentFrequency());
//            System.out.println("Key = "+key);
            InvertedList l = postings.get(key);
            InvertedList l1 = postings1.get(key);
            HashSet<String> docs = new HashSet<String>();
            HashSet<String> docs1 = new HashSet<String>();
           
            while(l.hasNext()) {
                assertTrue(l1.hasNext());    
                docs.add(new File(indexBuilder.getIdToDoc().get(l.advanceIterator())).getName());
                docs1.add(new File(indexBuilder1.getIdToDoc().get(l1.advanceIterator())).getName());
            }
            assertEquals(docs, docs1);
        }
       
	}
	
	@Test
	public void testIndex() throws Throwable {
        String base = prefix;
        prefix = base + "merged"+File.separator;
        FileUtils.deleteDirectory(prefix);
        FileUtils.mkdir(prefix);
        //fileWriter = new CatenaInvertedFileWriter();
        fileWriter = new RawInvertedFileWriter();
        tokenizer = new BasicTokenizer();
        InvertedFileBuilder indexBuilder = new InvertedFileBuilder(prefix, "corpus.index", fileWriter);
        fileReader = new InvertedFileReader();
        //tokenizer = new LuceneStandardTokenizer();
        corpusManager = new CorpusManager(prefix, tokenizer, indexBuilder);
        	    
	    createIndex("src/test/resources/text/split/");
        
        
        teardown();
        prefix = base + "single"+File.separator;
        FileUtils.deleteDirectory(prefix);
        FileUtils.mkdir(prefix);
        //fileWriter = new CatenaInvertedFileWriter();
        fileWriter = new RawInvertedFileWriter();
        tokenizer = new BasicTokenizer();
        InvertedFileBuilder indexBuilder1 = new InvertedFileBuilder(prefix, "corpus.index", fileWriter);
        fileReader = new InvertedFileReader();
        //tokenizer = new LuceneStandardTokenizer();
        corpusManager = new CorpusManager(prefix, tokenizer, indexBuilder1);

	   	createIndex("src/test/resources/text/all/");
	   	compare(indexBuilder, indexBuilder1);
	}
}
