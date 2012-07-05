package syndeticlogic.catena.array;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import org.apache.commons.io.DirectoryWalker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import syndeticlogic.catena.utility.CompositeKey;

import syndeticlogic.catena.store.Segment;
import syndeticlogic.catena.store.SegmentManager;

@SuppressWarnings({"rawtypes", "unchecked"})
public class Loader extends DirectoryWalker {
    private static final Log log = LogFactory.getLog(Loader.class);
    private Stack<ArrayDescriptor> current;
    
    public Loader() {
        super();
        current = new Stack<ArrayDescriptor>();
    }

    public List<ArrayDescriptor> load(String baseDir) {
        List<ArrayDescriptor> arrays = new LinkedList<ArrayDescriptor>();
        try {
            walk(new File(baseDir), arrays);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return arrays;
    }
        
    protected void handleDirectoryStart(File directory, int depth, Collection arrays) {
        assert depth <= 1; 
        String baseDir = directory.getAbsolutePath() + System.getProperty("file.separator");
        String arrayDesc = baseDir+ArrayDescriptor.ARRAY_DESC_FILE_NAME;
        try {
            File arrayDescFile = new File(arrayDesc);
            if(!arrayDescFile.exists()) {
                current.push(null);
                if(log.isTraceEnabled()) { 
                    log.trace("Loader.hahdleDirectoryStart: baseDir = "+baseDir + ","+ 
                            ArrayDescriptor.ARRAY_DESC_FILE_NAME);
                }
                return;
            }
                
            FileInputStream fi = new FileInputStream(arrayDescFile);
            byte[] arrayDescEncoded = new byte[fi.available()];
            fi.read(arrayDescEncoded);
            current.push(ArrayDescriptor.decode(arrayDescEncoded, 0));
            walk(arrayDescFile, arrays);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } 
    }

    protected void handleDirectoryEnd(File directory, int depth, Collection arrays) {
        if(current.peek() != null) {
            arrays.add(current.pop());
        } else {
            current.pop();
        }
    }
    
    protected void handleFile(File file, int depth, Collection arrays) {
        Segment s=null;
        
        if(current.peek() == null) {
            if(log.isTraceEnabled()) log.trace(file.getAbsolutePath());
            return;
        }

        if(ArrayDescriptor.ARRAY_DESC_FILE_NAME.equals(file.getName())) {
            return;
        }
        
        try {
            assert  SegmentManager.get() != null; 
            assert file != null; 
            assert current != null;
            s = SegmentManager.get().lookup(file.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        
        CompositeKey key = new CompositeKey();
        key.append(current.peek().id());
        key.append(file.getName());
        current.peek().addSegment(key, s);
    }
}
