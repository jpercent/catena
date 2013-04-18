package syndeticlogic.catena.text;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import syndeticlogic.catena.array.Array;
import syndeticlogic.catena.array.BinaryArray;
import syndeticlogic.catena.store.PageFactory.BufferPoolMemoryType;
import syndeticlogic.catena.store.PageFactory.CachingPolicy;
import syndeticlogic.catena.store.PageFactory.PageDescriptorType;
import syndeticlogic.catena.store.SegmentManager.CompressionType;
import syndeticlogic.catena.type.CodeableValue;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.CompositeKey;
import syndeticlogic.catena.utility.Config;

public class InvertedFileGenerator {
    private final HashMap<String, LinkedList<String>> blockToDocs;
    private final HashMap<Integer, String> idToDoc;
    private final TreeMap<Integer, String> idToWord;
    //private final TreeMap<Integer, >
    //private final HashMap<Integer, LinkedList<Integer>> wordToDocIds;
    //private HashMap<Integer, PriorityQueue<InvertedList>> idToPosting;
    private final TreeMap<String, InvertedList> postings;
    private String currentBlock;
    private LinkedList<File> blocks;
    private int docId;
    private int wordId;

    public InvertedFileGenerator() {
        blocks = new LinkedList<File>();
        blockToDocs = new HashMap<String, LinkedList<String>>();
        idToDoc = new HashMap<Integer, String>();
        idToWord = new TreeMap<Integer, String>();
        postings = new TreeMap<String, InvertedList>();
    }
    
    public void addWords(String directory, String fileName, HashSet<String> words) {
        System.out.println("Adding document documents... "+fileName);
        addDocument(directory, directory+File.separator+fileName);
        addWords(words);
        docId++;
        currentBlock = directory;
    }
    
    private void addDocument(final String directory, final String document) {

        idToDoc.put(docId, document);
        if(blockToDocs.containsKey(directory)) {
            blockToDocs.get(directory).add(document);
        } else {
            LinkedList<String> l = new LinkedList<String>() {{
                add(document);
            }};
        }  
    }
    
    private void addWords(HashSet<String> words) {
        for(String word : words) {
            InvertedList il = postings.get(word);
            if (il == null) {
                il = new InvertedList(wordId);
                il.setWord(word);
                postings.put(word, il);
                wordId++;
            }
            il.addDocumentId(docId);
        }
    }
    
    public void storeBlock() {
        System.out.println("Store block callled");
        Config config = new Config();
        try {
            config.configure(CompressionType.Null, "prefix", BufferPoolMemoryType.Java,
                    CachingPolicy.PinnableLru, PageDescriptorType.Unsynchronized, 
                    /* retrylimit */ 2, /* files */ null, /*pagesize */ 4096, /* percentageOfPhysicalMemory */ .40);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        CompositeKey key = new CompositeKey();
        key.append(config.prefix+File.separator+currentBlock);
        config.arrayRegistry.createArray(key, Type.CODEABLE);
        
        BinaryArray array = config.arrayRegistry.createArrayInstance(key);
        Array<CodeableValue> index = new Array<CodeableValue>(array);
        CodeableValue value = new CodeableValue();
        for(InvertedList postingsList : postings.values()) {
            System.out.println(postingsList.getWord());
            value.reset(postingsList);
            index.append(value);
        }
    }
    
    public void mergeBlocks() {
        System.out.println("Merge blocks called");
    } 
    
    public static void main(String[] args) {
        System.out.println("Start allocating... ");
        InvertedFileGenerator ifg = new InvertedFileGenerator();
        ifg.storeBlock();
        System.out.println("Done Allocating ");
    }
}
