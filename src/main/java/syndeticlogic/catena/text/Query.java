package syndeticlogic.catena.text;

import java.io.File;

import org.codehaus.plexus.util.FileUtils;

public class Query {
    private Tokenizer tokenizer;
    private InvertedFileWriter fileWriter;
    private InvertedFileReader fileReader;
    private InvertedFileBuilder indexBuilder;
    private CorpusManager corpusManager;
    private String prefix;
    private String[] terms;
    
    public Query(String prefix, String[] terms) throws Exception {
        this.prefix = prefix;
        fileReader = new InvertedFileReader();
        fileReader.open(prefix);
    }
    
    public void main(String[] args) throws Exception {
        Query q = new Query("target"+File.separator+"corpus-manager-block-merger-test"+File.separator+"merged/intermediate-merge-target.0");
    }
    
}
