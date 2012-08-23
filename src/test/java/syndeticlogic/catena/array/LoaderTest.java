package syndeticlogic.catena.array;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import syndeticlogic.catena.array.ArrayDescriptor;
import syndeticlogic.catena.array.Loader;
import syndeticlogic.catena.stubs.SegmentStub;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.type.TypeFactory;
import syndeticlogic.catena.utility.Codec;
import syndeticlogic.catena.utility.CompositeKey;

public class LoaderTest {
    String sep = System.getProperty("file.separator");
    String baseDir = "target" + sep + "loaderTest" + sep;
    String prefix = baseDir + "0";
    String prefix1 = baseDir + "1";
    
    @Before
    public void setup() throws Exception {
        try {
            FileUtils.forceDelete(new File(baseDir));
        } catch (Exception e) {
        }

        FileUtils.forceMkdir(new File(prefix));
        FileUtils.forceMkdir(new File(prefix1));        
        Codec.configureCodec(new TypeFactory());
    }

    public ArrayDescriptor createDesc(String prefix) {
        CompositeKey key = new CompositeKey();
        key.append(prefix);

        ArrayDescriptor adesc = new ArrayDescriptor(key, Type.INTEGER, 1048576);
        SegmentStub stub = new SegmentStub();
        stub.size = 32;
        stub.name = "arrayDescTest";
        adesc.addSegment(adesc.nextId(), stub);
        adesc.append(4);
        return adesc;
        
    }
    
    @Test
    public void test() throws Exception {

        ArrayDescriptor adesc = createDesc(prefix);
        adesc.persist();
        ArrayDescriptor adesc1 = createDesc(prefix1);
        adesc1.persist();
        
        Loader loader = new Loader();
        Collection<ArrayDescriptor> arrays = loader.load(baseDir);
        assertEquals(2, arrays.size());
        
    }
}
