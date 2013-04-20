package syndeticlogic.catena.text;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class InvertedFileBuilder {
	private static final Log log = LogFactory.getLog(InvertedFileBuilder.class);
	private final InvertedFileWriter fileWriter;
    private final HashMap<Integer, String> idToDoc;
    private final HashMap<Integer, String> idToWord;
    final TreeMap<String, InvertedList> postings;
    private final HashMap<Integer, Long> wordToOffset;
    private final HashMap<String, Integer> blockToId;
    private final String prefix;
    private int blockId;
    private int docId;
    private int wordId;

    public InvertedFileBuilder(String prefix, InvertedFileWriter fileWriter) {
        this.fileWriter = fileWriter;
        this.prefix = prefix;
        idToDoc = new HashMap<Integer, String>();
        idToWord = new HashMap<Integer, String>();
        postings = new TreeMap<String, InvertedList>();
        wordToOffset = new HashMap<Integer, Long>();
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
		fileWriter.writeFile(blockFileName, postings, wordToOffset);
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

	public HashMap<Integer, Long> getWordToOffset() {
		return wordToOffset;
	}

	public HashMap<String, Integer> getBlockToId() {
		return blockToId;
	}

	public String getPrefix() {
		return prefix;
	}

	public static void main(String[] args) {
        System.out.println("Start allocating... ");
        InvertedFileBuilder indexBuilder = new InvertedFileBuilder("target/InvertedFileBuilderTest", new RawInvertedFileWriter());
        System.out.println("Done Allocating ");
    }
}
