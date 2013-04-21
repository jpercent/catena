package syndeticlogic.catena.text;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import syndeticlogic.catena.utility.Config;

public class BlockMerger {
    private static final Log log = LogFactory.getLog(BlockMerger.class);
    private static int BLOCK_SIZE=10*1048576;
    private static double MEMORY_PERCENTAGE=0.3;
    private HashMap<Integer, String> idToWord;
    private String prefix;
    
    public BlockMerger(String prefix, HashMap<Integer, String> idToWord) {
        this.prefix = prefix;
        this.idToWord = idToWord;
    }
    
    public List<InvertedListDescriptor> mergeBlocks(String mergeTarget, LinkedList<Map.Entry<String, List<InvertedListDescriptor>>> blockDescriptors) {
        long memory = (long)(((double)Config.getPhysicalMemorySize()) * MEMORY_PERCENTAGE);
        int readBlocksAndWriteBlock = Math.min((int)(memory/BLOCK_SIZE), blockDescriptors.size()+1);
        int readBlocks = readBlocksAndWriteBlock - 1;
        int sources = blockDescriptors.size();
        
        String intermediateMerge = prefix+File.separator+"intermediate-merge-target.";
        List<InvertedListDescriptor> mergedDescriptors=null;
        String fileName=null;
        int totalMergeFiles = 0;

        while(sources > 0) {
            assert sources >= readBlocks;
            InvertedFileReader[] readers = new InvertedFileReader[readBlocks];
            List<InvertedListDescriptor>[] descriptors = new List[readBlocks];
            Iterator<Map.Entry<String, List<InvertedListDescriptor>>> bditerator = blockDescriptors.iterator();
            int i=0;
            if(mergedDescriptors != null) {
                assert fileName != null;
                descriptors[0] = mergedDescriptors;
                readers[0] = new InvertedFileReader();
                readers[0].setBlockSize(BLOCK_SIZE);
                readers[0].open(fileName);
                i = 1;
            }
            
            for(; i < readBlocks; i++) {
                Map.Entry<String, List<InvertedListDescriptor>> id = bditerator.next();
                descriptors[i] = id.getValue();
                readers[i] = new InvertedFileReader();
                readers[i].setBlockSize(BLOCK_SIZE);
                readers[i].open(id.getKey());
            }

            boolean finalPass = (((sources - readBlocks) > 0) ?  false : true);
            if(finalPass) {
                fileName = intermediateMerge+Integer.toString(totalMergeFiles);
            } else {
                fileName = mergeTarget;
            }
            InvertedFileWriter writer = new RawInvertedFileWriter();
            writer.setBlockSize(BLOCK_SIZE);
            writer.open(fileName);
            
            mergedDescriptors = merge(readers, descriptors, writer);
            totalMergeFiles += readBlocks;
            sources -= readBlocks;
            readBlocks = Math.min((int)(memory/BLOCK_SIZE), sources);
        }
        assert totalMergeFiles == blockDescriptors.size();
        for(int i = 0; i < totalMergeFiles; i++) {
            String name = intermediateMerge+Integer.toString(totalMergeFiles);
            if(!(new File(name).delete())) {
                log.warn("failed to delete "+name);
            }
        }
        return mergedDescriptors;
    }

    public List<InvertedListDescriptor> merge(InvertedFileReader[] readers, List<InvertedListDescriptor>[] descriptors, 
            InvertedFileWriter writer) {
        assert readers.length == descriptors.length;
        TreeMap<String, InvertedList> ret=null;
        int[] positions = new int[readers.length];
        
        for(int i = 0; i < readers.length; i++) {
            TreeMap<String, InvertedList> newPostings = new TreeMap<String, InvertedList>();
            positions[i] = readers[i].scanBlock(positions[i], descriptors[i], idToWord, newPostings);
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
        List<InvertedListDescriptor> newDescriptors = new LinkedList<InvertedListDescriptor>();
        writer.writeFile(ret, newDescriptors);
        return newDescriptors; 
    }
}
