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

import syndeticlogic.catena.text.io.BlockReader;
import syndeticlogic.catena.text.io.BlockWriter;
import syndeticlogic.catena.text.io.InvertedFileReadCursor;
import syndeticlogic.catena.text.io.old.InvertedFileReader;
import syndeticlogic.catena.text.io.old.InvertedFileWriter;
import syndeticlogic.catena.text.io.old.RawInvertedFileWriter;
import syndeticlogic.catena.text.postings.IdTable;
import syndeticlogic.catena.text.postings.InvertedFileBuilder;
import syndeticlogic.catena.text.postings.InvertedList;
import syndeticlogic.catena.text.postings.Tokenizer;

public class BlockMergerTest {
	Tokenizer tokenizer;
	BlockWriter fileWriter;
	BlockReader fileReader;
	InvertedFileBuilder indexBuilder;
	CorpusManager corpusManager;
	String prefix;
	
	@Before
    public void setup() throws Exception {
        prefix = "target"+File.separator+"corpus-manager-block-merger-test"+File.separator;
        FileUtils.deleteDirectory(prefix);
        FileUtils.mkdir(prefix);
        fileWriter = new BlockWriter();
        tokenizer = new BasicTokenizer();
        indexBuilder = new InvertedFileBuilder(prefix, fileWriter);
        fileReader = new BlockReader();
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
        corpusManager.index(source);
	}
	
	public void createFromSingleBlock() {
	}
	
	public void compare(InvertedFileBuilder indexBuilder, InvertedFileBuilder indexBuilder1) throws IOException {
        
        TreeMap<String, InvertedList> postings = new TreeMap<String, InvertedList>();
        fileReader.open("target/corpus-manager-block-merger-test/merged/corpus.index");
        InvertedFileReadCursor cursor = new InvertedFileReadCursor(indexBuilder.getIdToWord());
        fileReader.read(cursor);
        fileReader.close();
        postings = cursor.getInvertedList();
        
        TreeMap<String, InvertedList> postings1 = new TreeMap<String, InvertedList>();
        fileReader.open("target/corpus-manager-block-merger-test/single/corpus.index");
        cursor = new InvertedFileReadCursor(indexBuilder1.getIdToWord());
        fileReader.read(cursor);
        fileReader.close();
        postings1 = cursor.getInvertedList();

        
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
        InvertedList.setTableType(IdTable.TableType.VariableByteCodedTable);
        test();
	    
	}
	
	@Test
	public void testIndexUncoded() throws Throwable {
        InvertedList.setTableType(IdTable.TableType.VariableByteCodedTable);
        test();
	}
	
	public void test() throws Throwable {
        String base = prefix;
        prefix = base + "merged"+File.separator;
        FileUtils.deleteDirectory(prefix);
        FileUtils.mkdir(prefix);
        //fileWriter = new CatenaInvertedFileWriter();
        fileWriter = new BlockWriter();
        tokenizer = new BasicTokenizer();
        InvertedFileBuilder indexBuilder = new InvertedFileBuilder(prefix, fileWriter);
        fileReader = new BlockReader();
        //tokenizer = new LuceneStandardTokenizer();
        corpusManager = new CorpusManager(prefix, tokenizer, indexBuilder);
        	    
	    createIndex("src/test/resources/text/split/");
        
        
        teardown();
        prefix = base + "single"+File.separator;
        FileUtils.deleteDirectory(prefix);
        FileUtils.mkdir(prefix);
        //fileWriter = new CatenaInvertedFileWriter();
        fileWriter = new BlockWriter();
        tokenizer = new BasicTokenizer();
        InvertedFileBuilder indexBuilder1 = new InvertedFileBuilder(prefix, fileWriter);
        fileReader = new BlockReader();
        //tokenizer = new LuceneStandardTokenizer();
        corpusManager = new CorpusManager(prefix, tokenizer, indexBuilder1);

	   	createIndex("src/test/resources/text/all/");
	   	compare(indexBuilder, indexBuilder1);
	}
}
