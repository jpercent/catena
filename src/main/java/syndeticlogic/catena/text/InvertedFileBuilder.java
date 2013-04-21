package syndeticlogic.catena.text;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class InvertedFileBuilder {
	private static final Log log = LogFactory.getLog(InvertedFileBuilder.class);
    private final LinkedList<Map.Entry<String, List<InvertedListDescriptor>>> blocksAndDescriptors;
    private final InvertedFileWriter fileWriter;
    private final HashMap<Integer, String> idToDoc;
    private final HashMap<Integer, String> idToWord;
    private final HashMap<String, Integer> wordToId;
    private final HashMap<String, Integer> blockToId;
    private final String prefix;
    private TreeMap<String, InvertedList> postings;
    private int blockId;
    private int docId;
    private int wordId;
    
    public InvertedFileBuilder(String prefix, String corpusName, InvertedFileWriter fileWriter) {
        this.fileWriter = fileWriter;
        this.prefix = prefix;
        idToDoc = new HashMap<Integer, String>();
        idToWord = new HashMap<Integer, String>();
        postings = new TreeMap<String, InvertedList>();
        blocksAndDescriptors = new LinkedList<Map.Entry<String, List<InvertedListDescriptor>>>();
        blockToId = new HashMap<String, Integer>();
        wordToId = new HashMap<String, Integer>();
    }
        
    public void addWord(int document, String word) {
        InvertedList invertedList = postings.get(word);
        if (invertedList == null) {
            if(wordToId.get(word) == null) {
                wordToId.put(word, wordId);
                idToWord.put(wordId, word);
                wordId++;
            }            
            invertedList = new InvertedList(wordToId.get(word));
            invertedList.setWord(word);
            postings.put(word, invertedList);
        }
        invertedList.addDocumentId(document);
    }

    public int addDocument(final String document) {
        idToDoc.put(docId, document);
        return docId++;
    }

    public void startBlock(String block) {
    	blockToId.put(block, blockId++);
	}
   
	public void completeBlock(String block) {
	    File blockFile = new File(block);
		String blockFileName = prefix+File.separator+blockFile.getName()+"-"+blockToId.get(block)+".corpus";
		fileWriter.open(blockFileName);
		LinkedList<InvertedListDescriptor> invertedListDescriptors = new LinkedList<InvertedListDescriptor>();   
		fileWriter.writeFile(postings, invertedListDescriptors);
        fileWriter.close();
        this.blocksAndDescriptors.add(new AbstractMap.SimpleEntry<String, List<InvertedListDescriptor>>(blockFileName, invertedListDescriptors));
        postings = new TreeMap<String, InvertedList>();
	}

    public void mergeBlocks() {
        List<InvertedListDescriptor> finalList;
        if(blocksAndDescriptors.size() > 1) {
            BlockMerger merger = new BlockMerger(prefix, idToWord);
            finalList = merger.mergeBlocks("final.index", blocksAndDescriptors);
        } else {
            finalList = (List<InvertedListDescriptor>) blocksAndDescriptors.get(0).getValue();
        }
        writeMeta(finalList, idToDoc);
    }
    
    public void writeMeta(Object... objects) {
        File file = new File(prefix+File.separator+".meta");
        ObjectOutputStream oos;
        try {
            oos = new ObjectOutputStream(new FileOutputStream(file));
            oos.writeObject(objects);
            oos.close();
        } catch (Exception e) {
            log.fatal("Could not write meta "+file.getAbsolutePath()+": "+e, e);
            throw new RuntimeException(e);
        }
        
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
		return null;//blockToInvertedListDescriptor.get(corpusName);
	}

	public HashMap<String, Integer> getBlockToId() {
		return blockToId;
	}

	public String getPrefix() {
		return prefix;
	}
}
