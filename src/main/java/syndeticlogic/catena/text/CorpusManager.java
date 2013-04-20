package syndeticlogic.catena.text;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.DirectoryWalker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@SuppressWarnings({"rawtypes", "unchecked"})
public class CorpusManager extends DirectoryWalker {
    private static final Log log = LogFactory.getLog(CorpusManager.class);
    private InvertedFileBuilder indexBuilder;
    private Tokenizer tokenizer;
    
    public CorpusManager(Tokenizer tokenizer) {
        super();
        this.indexBuilder = new InvertedFileBuilder();
        this.tokenizer = tokenizer;
    }

    public void index(String baseDirectory) {
        try {
        	File baseDirectoryFile = new File(baseDirectory);
        	assert baseDirectoryFile.exists() && baseDirectoryFile.isDirectory();
            walk(baseDirectoryFile, null);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        indexBuilder.mergeBlocks();
    }

    @Override
    protected void handleDirectoryStart(File directory, int depth, Collection wrapper) {
    	System.out.println("Depth = "+depth);
    	if(depth == 1) {
    		System.out.println("Depth = "+depth+ " " +directory.getAbsolutePath());
    		indexBuilder.startBlock(directory.getAbsolutePath());
    	}
    }
    
    @Override
    protected void handleDirectoryEnd(File directory, int depth, Collection wrapper) {
    	if(depth == 1) {
    		indexBuilder.completeBlock(directory.getAbsolutePath());
        	throw new RuntimeException("Stop");
        }
    }
    
    @Override
    protected void handleFile(File file, int depth, Collection wrapper) {
    	int document = indexBuilder.addDocument(file.getAbsolutePath());
    	tokenizer.tokenize(indexBuilder, file, document);
    	//System.out.println(file.getAbsolutePath());
    }
    public static void main(String[] args) {
    	CorpusManager corpusManager = new CorpusManager(new BasicTokenizer());
    	//CorpusManager corpusManager = new CorpusManager(new LuceneStandardTokenizer());
    	try {
    		corpusManager.index("/home/james/catena/PA1/data");
    	} catch(Exception e) {}	
    	System.out.println(corpusManager.indexBuilder.postings.keySet().size());
    }
}