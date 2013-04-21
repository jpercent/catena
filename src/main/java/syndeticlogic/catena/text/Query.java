package syndeticlogic.catena.text;

import java.io.File;

import org.codehaus.plexus.util.FileUtils;

public class Query {
    Tokenizer tokenizer;
    InvertedFileWriter fileWriter;
    InvertedFileReader fileReader;
    InvertedFileBuilder indexBuilder;
    CorpusManager corpusManager;
    String prefix;
    
    public Query(String prefix) throws Exception {
        this.prefix = prefix;
        FileUtils.deleteDirectory(prefix);
        FileUtils.mkdir(prefix);
        fileWriter = new RawInvertedFileWriter();
        tokenizer = new BasicTokenizer();
        indexBuilder = new InvertedFileBuilder(prefix, "corpus.index", fileWriter);
        fileReader = new InvertedFileReader();
        
    }
    
    public void main(String[] args) throws Exception {
        Query q = new Query("target"+File.separator+"corpus-manager-block-merger-test"+File.separator+"merged/intermediate-merge-target.0");
    }
    
}
