package syndeticlogic.catena.text.io.old;

import static org.junit.Assert.*;

import java.io.File;

import org.codehaus.plexus.util.FileUtils;
import org.junit.Before;
import org.junit.Test;

import syndeticlogic.catena.text.BasicTokenizer;
import syndeticlogic.catena.text.CorpusManager;
import syndeticlogic.catena.text.io.InvertedFileWriter;
import syndeticlogic.catena.text.io.RawInvertedFileWriter;
import syndeticlogic.catena.text.postings.InvertedFileBuilder;
import syndeticlogic.catena.text.postings.Tokenizer;

public class CatenaInvertedWriterTest {
	Tokenizer tokenizer;
	InvertedFileWriter fileWriter;
	InvertedFileBuilder indexBuilder;
	CorpusManager corpusManager;
	String prefix;
	
	@Before
	public void setup() throws Exception {
    	prefix = "target"+File.separator+"catena-inverted-writer"+File.separator;
    	FileUtils.deleteDirectory(prefix);
    	FileUtils.mkdir(prefix);
    	//fileWriter = new CatenaInvertedFileWriter();
    	fileWriter = new RawInvertedFileWriter();
    	tokenizer = new BasicTokenizer();
    	indexBuilder = new InvertedFileBuilder(prefix, fileWriter);
    	//tokenizer = new LuceneStandardTokenizer();
    	corpusManager = new CorpusManager(prefix, tokenizer, indexBuilder);
	}
	
	
	@Test
	public void testIndex() {
	   	long start = System.currentTimeMillis();
	   	corpusManager.index("src/test/resources/text/some/");
    	System.out.println("Total time = "+(System.currentTimeMillis() - start));
    	System.out.println(indexBuilder.getPostings().keySet().size());
	}
}
