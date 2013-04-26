package syndeticlogic.catena.text;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import syndeticlogic.catena.text.io.BlockReader;
import syndeticlogic.catena.text.io.BlockWriter;
import syndeticlogic.catena.text.io.InvertedFileReadCursor;
import syndeticlogic.catena.text.io.InvertedFileWriteCursor;
import syndeticlogic.catena.text.postings.InvertedList;
import syndeticlogic.catena.text.postings.InvertedListDescriptor;

public class BlockMerger {
    private static int BLOCK_SIZE=100*1048576;
    private HashMap<Integer, String> idToWord;
    
    public BlockMerger(HashMap<Integer, String> idToWord) {
        this.idToWord = idToWord;
    }
    
    public List<InvertedListDescriptor> mergeBlocks(String mergeTarget, LinkedList<String> blocks) {
        System.err.println("Starting merge...");
        String fileName = null;
        List<InvertedListDescriptor> ret=null;
                        
        BlockReader[] readers = new BlockReader[blocks.size()];
        InvertedFileReadCursor[] cursors = new InvertedFileReadCursor[blocks.size()];

        Iterator<String> blockFilesIter = blocks.iterator();
        for(int i = 0; i < blocks.size(); i++) {
            assert blockFilesIter.hasNext();
            String blockFile = blockFilesIter.next();
            System.err.println("Block order = "+i + " = "+blockFile);
            cursors[i] = new InvertedFileReadCursor(idToWord);
                readers[i] = new BlockReader();
                readers[i].setBlockSize(BLOCK_SIZE);
                readers[i].open(blockFile);
            }
        fileName = mergeTarget;
        System.err.println("Merging...");
        ret = merge(fileName, readers, cursors);
        for(int i = 0; i < blocks.size(); i++) {
            readers[i].close();
        }
        return ret;
    }

    public List<InvertedListDescriptor> merge(String target, BlockReader[] readers, InvertedFileReadCursor[] cursors) {
        boolean done = false;
        List<InvertedListDescriptor> ret = new LinkedList<InvertedListDescriptor>();;
        BlockWriter writer = new BlockWriter();
        writer.setBlockSize(BLOCK_SIZE);
        writer.open(target);

        while (!done) {
            done = true;
            TreeMap<String, InvertedList> mergedPostings = null;
            for (int i = 0; i < readers.length; i++) {

                if (readers[i].hasMore()) {
                    done = false;
                    readers[i].readNextBlock(cursors[i]);
                    TreeMap<String, InvertedList> newPostings = cursors[i].getInvertedList();
                    assert newPostings != null;

                    if (mergedPostings == null) {
                        mergedPostings = newPostings;
                    } else {
                        for (InvertedList list : newPostings.values()) {
                            InvertedList retList = mergedPostings.get(list.getWord());
                            if (retList == null) {
                                mergedPostings.put(list.getWord(), list);
                            } else {
                                assert retList.getLastDocId() < list.getFirstDocId();
                                retList.merge(list);
                            }
                        }
                    }
                }
                System.err.println("Memory used before cursor write = "+(double)Runtime.getRuntime().totalMemory()+":"+Runtime.getRuntime().freeMemory());
            }
            if (!done) {
                InvertedFileWriteCursor cursor = new InvertedFileWriteCursor(mergedPostings);
                writer.writeBlock(cursor);
                ret.addAll(cursor.getInvertedListDescriptors());
                mergedPostings = null;
            }
            System.err.println("Memory used after cursor write = "+(double)Runtime.getRuntime().totalMemory()+":"+Runtime.getRuntime().freeMemory());
        }
        writer.close();
        return ret;
    }
}
    
