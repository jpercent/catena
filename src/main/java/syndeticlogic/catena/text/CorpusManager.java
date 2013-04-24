package syndeticlogic.catena.text;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.DirectoryWalker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.plexus.util.FileUtils;

import syndeticlogic.catena.text.io.InvertedFileReader;
import syndeticlogic.catena.text.io.InvertedFileWriter;
import syndeticlogic.catena.text.io.RawInvertedFileWriter;
import syndeticlogic.catena.text.postings.InvertedFileBuilder;
import syndeticlogic.catena.text.postings.InvertedList;
import syndeticlogic.catena.text.postings.Tokenizer;

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
    
    public static void index(String corpus, String output, boolean clearOutputDir) throws Throwable {
        Tokenizer tokenizer;
        InvertedFileWriter fileWriter;
        InvertedFileReader fileReader;
        InvertedFileBuilder indexBuilder;
        CorpusManager corpusManager;
        String prefix;

        String base = output;
        prefix = base + File.separator;
        if (clearOutputDir) {
            FileUtils.deleteDirectory(prefix);
        }
        FileUtils.mkdir(prefix);
        
        fileWriter = new RawInvertedFileWriter();
        tokenizer = new BasicTokenizer();
        
        indexBuilder = new InvertedFileBuilder(prefix, fileWriter);
        fileReader = new InvertedFileReader();
        corpusManager = new CorpusManager(prefix, tokenizer, indexBuilder);
        
        corpusManager.index(corpus);
        System.out.println(indexBuilder.getNumberOfDocumentsIndexed()+"\n");
        fileWriter.close();
        fileWriter = null;
        tokenizer = null;
        indexBuilder = null;
        if (fileReader != null)
            fileReader.close();
        fileReader = null;
        corpusManager = null;
    }
    
    public static void main(String[] args) {
        CorpusManagerConfig corpusManagerConfig=null;
        long start=0;
        try {
            
            corpusManagerConfig = new CorpusManagerConfig(args);
            boolean success = corpusManagerConfig.parse();
            if (!success) {
                throw new RuntimeException("Invalid parameters");
            }
            start = System.currentTimeMillis();
            File test = new File(corpusManagerConfig.getInputPrefix());
            if(!test.exists()) {
                throw new RuntimeException("Input prefix must be a valid directory");
            } 
            if(corpusManagerConfig.getInputPrefix().equals(corpusManagerConfig.getOutputPrefix())) {
                throw new RuntimeException("Input and output prefix cannot be the same");
            }
            System.err.println("Tabl type ="+corpusManagerConfig.getTableType());
            InvertedList.setTableType(corpusManagerConfig.getTableType());
            CorpusManager.index(corpusManagerConfig.getInputPrefix(), corpusManagerConfig.getOutputPrefix(), corpusManagerConfig.removeOutputPrefix());
            System.err.println("Total time = "+(System.currentTimeMillis() - start));
        } catch(Throwable t) {
            String prefix = (corpusManagerConfig != null ? corpusManagerConfig.getInputPrefix() : "<no file name was parsed> ");
            System.err.println("Exception indexing corpus " + prefix + " message ");
            t.printStackTrace(System.err);
        }

    }
}

