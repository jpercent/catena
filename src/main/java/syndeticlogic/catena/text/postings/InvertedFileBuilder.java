package syndeticlogic.catena.text.postings;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import org.codehaus.plexus.util.FileUtils;

import syndeticlogic.catena.text.BlockMerger;
import syndeticlogic.catena.text.io.BlockWriter;
import syndeticlogic.catena.text.io.DictionaryWriteCursor;
import syndeticlogic.catena.text.io.InvertedFileWriteCursor;

public class InvertedFileBuilder {
    private final LinkedList<String> blocks;
    private List<InvertedListDescriptor> descriptors;
    private final BlockWriter blockWriter;
    private final HashMap<Integer, String> idToDoc;
    private final HashMap<Integer, String> idToWord;
    private final HashMap<String, Integer> wordToId;
    private final String prefix;
    private TreeMap<String, InvertedList> postings;
    private int docId;
    private int wordId;
    
    public InvertedFileBuilder(String prefix, BlockWriter fileWriter) {
        this.blockWriter = fileWriter;
        this.prefix = prefix;
        blocks = new LinkedList<String>();
        idToDoc = new HashMap<Integer, String>();
        idToWord = new HashMap<Integer, String>();
        postings = new TreeMap<String, InvertedList>();
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
        return docId++;
    }

    public void startBlock(String block) {
        System.err.println("Starting block "+block+" starting document id = "+docId);
    	postings = new TreeMap<String, InvertedList>();
	}
  
	public void completeBlock(String block) {
        System.err.println("Complete block "+block+ " writing intermediates "+" ending document id = "+docId);
        
        String blockFileName = getBlockFileName(block);
        System.err.println("BLOCK FILE NAME == "+blockFileName);
	    blockWriter.open(blockFileName);
		InvertedFileWriteCursor cursor = new InvertedFileWriteCursor(postings);
		blockWriter.writeFile(cursor);
        blockWriter.close();
        descriptors = cursor.getInvertedListDescriptors();
        cursor = null;
        this.blocks.add(blockFileName);
	}

    public void mergeBlocks() {
        System.err.println("Merging blocks... ");
        if(blocks.size() > 1) {
            BlockMerger merger = new BlockMerger(idToWord);
            descriptors = merger.mergeBlocks(getFinalName(), blocks);
        } else {
            try {
                FileUtils.rename(new File(blocks.get(0)), new File(getFinalName()));
            } catch (IOException e) {
                System.err.println("could not rename index file; final index is named "+blocks.get(0));
            }
        }
        System.err.println("All blocks merged... ");
        writeMeta(descriptors);
    }

    private String getFinalName() {
        return prefix+File.separator+"corpus-"+InvertedList.getTableType().name()+".index";
    }

    private String getDictionaryName() {
        return prefix+File.separator+"index.meta";
    }
    
    private String getBlockFileName(String block) {
        File blockFile = new File(block);
        return prefix+"."+blockFile.getParentFile().getName()+"-"+InvertedList.getTableType().name()
                +"-"+blockFile.getName()+".index";
    }
    
    public void writeMeta(List<InvertedListDescriptor> finalList) {
        DictionaryWriteCursor cursor = new DictionaryWriteCursor(idToDoc, finalList);
        blockWriter.open(getDictionaryName());
        blockWriter.writeFile(cursor);
        blockWriter.close();
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


	public String getPrefix() {
		return prefix;
	}

    public int getNumberOfDocumentsIndexed() {
        return docId;
    }
}
