package syndeticlogic.catena.text.postings;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.plexus.util.FileUtils;

import syndeticlogic.catena.text.BlockMerger;
import syndeticlogic.catena.text.io.DictionaryWriter;
import syndeticlogic.catena.text.io.InvertedFileWriter;

public class InvertedFileBuilder {
	private static final Log log = LogFactory.getLog(InvertedFileBuilder.class);
    private final LinkedList<Map.Entry<String, List<InvertedListDescriptor>>> blocksAndDescriptors;
    private List<InvertedListDescriptor> descriptors;
    private final InvertedFileWriter fileWriter;
    private final HashMap<Integer, String> idToDoc;
    private final HashMap<Integer, String> idToWord;
    private final HashMap<String, Integer> wordToId;
    private final HashMap<String, Integer> blockToId;
    private final HashMap<String, Integer> directoryToId;
    private final String prefix;
    private TreeMap<String, InvertedList> postings;
    private int blockId;
    private int docId;
    private int directoryId;
    private int wordId;
    
    public InvertedFileBuilder(String prefix, InvertedFileWriter fileWriter) {
        this.fileWriter = fileWriter;
        this.prefix = prefix;
        idToDoc = new HashMap<Integer, String>();
        idToWord = new HashMap<Integer, String>();
        postings = new TreeMap<String, InvertedList>();
        blocksAndDescriptors = new LinkedList<Map.Entry<String, List<InvertedListDescriptor>>>();
        blockToId = new HashMap<String, Integer>();
        directoryToId = new HashMap<String, Integer>();
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
            invertedList = InvertedList.create(wordToId.get(word));
            invertedList.setWord(word);
            postings.put(word, invertedList);
        }
        invertedList.addDocumentId(document);
    }

    public int addDocument(final String document) {
        idToDoc.put(docId, document);
        File f = new File(document);
        if(!directoryToId.containsKey(f.getParent())) {
            directoryToId.put(f.getParent(), directoryId++);
        }
        return docId++;
    }

    public void startBlock(String block) {
        System.err.println("Starting block "+block);
    	blockToId.put(block, blockId++);
    	postings = new TreeMap<String, InvertedList>();
	}
   
    private String getBlockFileName(String block) {
        File blockFile = new File(block);
        return prefix+File.separator+"."+blockFile.getName()+"-block-"+blockToId.get(block)+".index";
    }
    
	public void completeBlock(String block) {
        System.err.println("Complete block "+block+ " writing intermediates ");
	    String blockFileName = getBlockFileName(block);
		fileWriter.open(blockFileName);
		LinkedList<InvertedListDescriptor> invertedListDescriptors = new LinkedList<InvertedListDescriptor>();   
		fileWriter.writeFile(postings, invertedListDescriptors);
        fileWriter.close();
        this.blocksAndDescriptors.add(new AbstractMap.SimpleEntry<String, List<InvertedListDescriptor>>(blockFileName, invertedListDescriptors));
        System.err.println("Block complete");
	}

    public void mergeBlocks() {
        System.err.println("Merging blocks... ");
        List<InvertedListDescriptor> finalList;
        if(blocksAndDescriptors.size() > 1) {
            BlockMerger merger = new BlockMerger(prefix, idToWord);
            descriptors = merger.mergeBlocks(getFinalName(), blocksAndDescriptors);
        } else {
            descriptors = (List<InvertedListDescriptor>) blocksAndDescriptors.get(0).getValue();
            Map.Entry<String,  List<InvertedListDescriptor>> blockDesc = blocksAndDescriptors.get(0);
            try {
                FileUtils.rename(new File(blockDesc.getKey()), new File(getFinalName()));
            } catch (IOException e) {
                System.err.println("could not rename index file; final index is named "+blockDesc.getKey());
            }
        }
        System.err.println("All blocks merged... ");
        writeMeta(descriptors);
    }

    private String getFinalName() {
        return prefix+File.separator+"corpus.index";
    }

    private String getDictionaryName() {
        return prefix+File.separator+"index.meta";
    }

    public void writeMeta(List<InvertedListDescriptor> finalList) {
        DictionaryWriter writer = new DictionaryWriter();
        writer.open(getDictionaryName());
        writer.writeDictionary(idToDoc, directoryToId, finalList);
        writer.close();
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

	public List<InvertedListDescriptor> getInvertedListDescriptors() {
		return descriptors;
	}

	public HashMap<String, Integer> getBlockToId() {
		return blockToId;
	}

	public String getPrefix() {
		return prefix;
	}

    public int getNumberOfDocumentsIndexed() {
        return docId;
    }
}
