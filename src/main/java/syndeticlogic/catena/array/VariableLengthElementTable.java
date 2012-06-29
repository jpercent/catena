package syndeticlogic.catena.array;

//import java.io.File;
//import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import syndeticlogic.catena.utility.CompositeKey;

public class VariableLengthElementTable implements ElementTable {
    //private static final Log log = LogFactory.getLog(VariableLengthElementTable.class);
    private HashMap<CompositeKey, List<Integer>> meta;
    //private File elementFile;

    public VariableLengthElementTable(CompositeKey key) {
        meta = new HashMap<CompositeKey, List<Integer>>();
        /*String metaFileName = key.toString()+System.getProperty("file.separator")+".varmeta";
        
        log.trace("creating meta file"+metaFileName);
        
        elementFile = new File(metaFileName);
        if(!elementFile.exists()) {
            log.info("creating meta file "+metaFileName);
            try {
                elementFile.createNewFile();
            } catch (IOException e) {
                log.error("error creating file "+metaFileName, e);
                throw new RuntimeException(e);
            }
        }*/
    }

    @Override
    public ElementDescriptor find(long index) {
        ElementDescriptor eDesc=null;
        long elements = 0;

        for(Entry<CompositeKey, List<Integer>>  segmentMeta : meta.entrySet()) {
            List<Integer> offsets = segmentMeta.getValue();
            CompositeKey id = segmentMeta.getKey();
            assert offsets != null && id != null;            
            int segmentCardinality = offsets.size();
            
            if(elements + segmentCardinality >= index) {
                eDesc = locateElement(offsets, id, elements, index);
                break;
            }
            elements += segmentCardinality;
        }
        return eDesc;
    }

    @Override
    public int update(long index, int size) {
        return -1;
    }

    @Override
    public void append(int size) {
    }

    @Override
    public int delete(long index) {
        return -1;
    }

    @Override
    public void persist() {
    }
    
    private ElementDescriptor locateElement(List<Integer> offsets, CompositeKey id,
                                            long elements, long index) {
        int segmentOffset = 0;
        int size = 0;
        ElementDescriptor eDesc=null;
        
        for(Integer i : offsets) {
            elements++;
            size = i.intValue();
            if(elements == index) {
                eDesc = new ElementDescriptor(id, segmentOffset, size, index);
                break;
            }
            segmentOffset += size;
        }
        return eDesc;   
    }
}