package syndeticlogic.catena.text;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class InvertedFileBuilder {
	private static final Log log = LogFactory.getLog(InvertedFileBuilder.class);
	private final InvertedFileWriter fileWriter;
    private final HashMap<Integer, String> idToDoc;
    private final HashMap<Integer, String> idToWord;
    final TreeMap<String, InvertedList> postings;
    private final HashMap<String, LinkedList<InvertedListDescriptor>> blockToInvertedListDescriptor;
    private final HashMap<String, Integer> blockToId;
    private final String prefix;
    private final String corpusName;
    private int blockId;
    private int docId;
    private int wordId;
    
    public InvertedFileBuilder(String prefix, String corpusName, InvertedFileWriter fileWriter) {
        this.fileWriter = fileWriter;
        this.prefix = prefix;
        this.corpusName = prefix+File.separator+corpusName;
        idToDoc = new HashMap<Integer, String>();
        idToWord = new HashMap<Integer, String>();
        postings = new TreeMap<String, InvertedList>();
        blockToInvertedListDescriptor = new HashMap<String, LinkedList<InvertedListDescriptor>>();
        blockToId = new HashMap<String, Integer>();
    }
        
    public void addWord(int document, String word) {
        InvertedList il = postings.get(word);
        if (il == null) {
            il = new InvertedList(wordId);
            il.setWord(word);
            postings.put(word, il);
            idToWord.put(wordId, word);
            wordId++;
        }
        il.addDocumentId(document);
    }

    public int addDocument(final String document) {
        idToDoc.put(docId, document);
        return docId++;
    }

    public void startBlock(String block) {
    	blockToId.put(block, blockId++);
	}
   
	public void completeBlock(String block) {
		String blockFileName = prefix+File.separator+new File(block).getName()+"-"+blockToId.get(block)+".corpus";
		fileWriter.open(blockFileName);
		LinkedList<InvertedListDescriptor> invertedListDescriptors = new LinkedList<InvertedListDescriptor>();   
		fileWriter.write(postings, invertedListDescriptors);
        fileWriter.close();
        this.blockToInvertedListDescriptor.put(block, invertedListDescriptors);

	}

    public void mergeBlocks() {
        System.out.println("Merge blocks called");
    } 
    
    public HashMap<Integer, String> getIdToDoc() {
		return idToDoc;
	}

	public HashMap<Integer, String> getIdToWord() {
		return idToWord;
	}

	public TreeMap<String, InvertedList> getPostings() {
		return postings;
	}

	public LinkedList<InvertedListDescriptor> getInvertedListDescriptors() {
		return blockToInvertedListDescriptor.get(corpusName);
	}

	public HashMap<String, Integer> getBlockToId() {
		return blockToId;
	}

	public String getPrefix() {
		return prefix;
	}

	public static void main(String[] args) {
        System.out.println("Start allocating... ");
        InvertedFileBuilder indexBuilder = new InvertedFileBuilder("target/InvertedFileBuilderTest", "index.corpus", new RawInvertedFileWriter());
        System.out.println("Done Allocating ");
    }
}
