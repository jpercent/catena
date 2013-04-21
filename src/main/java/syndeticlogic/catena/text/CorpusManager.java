package syndeticlogic.catena.text;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.DirectoryWalker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.plexus.util.FileUtils;

@SuppressWarnings({"rawtypes", "unchecked"})
public class CorpusManager extends DirectoryWalker {
    private static final Log log = LogFactory.getLog(CorpusManager.class);
    private InvertedFileBuilder indexBuilder;
    private Tokenizer tokenizer;
    
    public CorpusManager(String prefix, Tokenizer tokenizer, InvertedFileBuilder indexBuilder) {
        super();
        this.indexBuilder = indexBuilder;
        this.tokenizer = tokenizer;
    }

    public void index(String baseDirectory) {
        try {
        	File baseDirectoryFile = new File(baseDirectory);
        	assert baseDirectoryFile.exists() && baseDirectoryFile.isDirectory();
            walk(baseDirectoryFile, null);
        } catch (IOException e) {
            log.fatal("could not index "+baseDirectory+e, e);
            throw new RuntimeException(e);
        }
        indexBuilder.mergeBlocks();
    }

    @Override
    protected void handleDirectoryStart(File directory, int depth, Collection wrapper) {
    	if(depth == 1) {
    		indexBuilder.startBlock(directory.getAbsolutePath());
    	}
    }
    
    @Override
    protected void handleDirectoryEnd(File directory, int depth, Collection wrapper) {
    	if(depth == 1) {
    		indexBuilder.completeBlock(directory.getAbsolutePath());
        }
    }
    
    @Override
    protected void handleFile(File file, int depth, Collection wrapper) {
    	int document = indexBuilder.addDocument(file.getAbsolutePath());
    	tokenizer.tokenize(indexBuilder, file, document);
    }
    
    public static void main(String[] args) {
        //long start = System.currentTimeMillis();
    	String prefix = "target"+File.separator+"corpus-manager"+File.separator;
    	FileUtils.mkdir(prefix);
    	InvertedFileWriter fileWriter = new RawInvertedFileWriter();
    	Tokenizer tokenizer = new BasicTokenizer();
    	CorpusManager corpusManager = new CorpusManager(prefix, tokenizer, new InvertedFileBuilder(prefix, "corpus.index", fileWriter));
    	try {
    		corpusManager.index("/home/james/catena/PA1/data");
    	} catch(Throwable e) {e.printStackTrace();}
    	//System.out.println("Total time = "+(System.currentTimeMillis() - start));
    	//System.out.println(corpusManager.indexBuilder.getPostings().keySet().size());
    	
    }
}
