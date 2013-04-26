package syndeticlogic.catena.text;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import syndeticlogic.catena.text.io.BlockReader;
import syndeticlogic.catena.text.io.BlockWriter;
import syndeticlogic.catena.text.io.InvertedFileReadCursor;
import syndeticlogic.catena.text.io.InvertedFileReader;
import syndeticlogic.catena.text.io.InvertedFileWriteCursor;
import syndeticlogic.catena.text.io.InvertedFileWriter;
import syndeticlogic.catena.text.io.RawInvertedFileWriter;
import syndeticlogic.catena.text.postings.InvertedList;
import syndeticlogic.catena.text.postings.InvertedListDescriptor;
import syndeticlogic.catena.utility.Config;

public class BlockMerger {
    private static int BLOCK_SIZE=100*1048576;
    private static double MEMORY_PERCENTAGE=0.3;
    private HashMap<Integer, String> idToWord;
    private String prefix;
    
    public BlockMerger(String prefix, HashMap<Integer, String> idToWord) {
        this.prefix = prefix;
        this.idToWord = idToWord;
    }
    
    public List<InvertedListDescriptor> mergeBlocks(String mergeTarget, LinkedList<String> blocks) {
        System.err.println("Starting merge...");
        boolean done = false;
        int merges = 0;
        String fileName = null;
        List<InvertedListDescriptor> ret=null;
        
        while(!done) {
        
            long memory = (long)(((double)Config.getPhysicalMemorySize()) * MEMORY_PERCENTAGE);
            int readBlocksAndWriteBlock = Math.min((int)(memory/BLOCK_SIZE), blocks.size()+1);
            int readBlocks = readBlocksAndWriteBlock - 1;
        
            BlockReader[] readers = new BlockReader[readBlocks];
            InvertedFileReadCursor[] cursors = new InvertedFileReadCursor[readBlocks];

            Iterator<String> blockFilesIter = blocks.iterator();
            for(int i = 0; i < readBlocks; i++) {
                assert blockFilesIter.hasNext();
                cursors[i] = new InvertedFileReadCursor(idToWord);
                readers[i] = new BlockReader();
                readers[i].setBlockSize(BLOCK_SIZE);
                readers[i].open(blockFilesIter.next());
            }
                       
            if(!blockFilesIter.hasNext()) {
                done = true;
                fileName = mergeTarget;
            } else {
                LinkedList<String> remaining = new LinkedList<String>();
                String mergeFileName = prefix+File.separator+"intermediate-merge-target."+Integer.toString(merges++);
                remaining.add(mergeFileName);
                while(blockFilesIter.hasNext()) {
                    remaining.add(blockFilesIter.next());
                }
                blocks = remaining;
                fileName = mergeFileName;
            }

            ret = merge(fileName, readers, cursors);
            for(int i = 0; i < readBlocks; i++) {
                readers[i].close();
            }
        }
        return ret;
    }

    public List<InvertedListDescriptor> merge(String target, BlockReader[] readers, InvertedFileReadCursor[] cursors) {
        TreeMap<String, InvertedList> ret=null;
        for(int i = 0; i < readers.length; i++) {
            readers[i].scanBlock(cursors[i]);
            TreeMap<String, InvertedList> newPostings = cursors[i].getInvertedList();
            if(ret == null) {
                ret = newPostings;
            } else {
                for(InvertedList list : newPostings.values()) {
                    InvertedList retList = ret.get(list.getWord());
                    if(retList == null) {
                        ret.put(list.getWord(), list);
                    } else {
                        retList.merge(list);
                    }
                }
            }
        }
        BlockWriter writer = new BlockWriter();
        writer.setBlockSize(BLOCK_SIZE);
        writer.open(target);
        
        //List<InvertedListDescriptor> newDescriptors = new LinkedList<InvertedListDescriptor>();
        InvertedFileWriteCursor cursor = new InvertedFileWriteCursor(ret);
        writer.writeBlock(cursor);
        List<InvertedListDescriptor> newDescriptors = cursor.getInvertedListDescriptors();
        writer.close();
        return newDescriptors; 
    }
}
