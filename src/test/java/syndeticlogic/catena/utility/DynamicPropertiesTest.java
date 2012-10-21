package syndeticlogic.catena.utility;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import org.apache.commons.io.FilenameUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DynamicPropertiesTest implements SimpleNotificationListener {
    String directory = FilenameUtils.concat(FilenameUtils.concat(FilenameUtils.concat(new File(".").getAbsolutePath(), "src"), "test"), "resources");
    File testFile = new File("src/test/resources/testproperties.properties");
    File testFile1 = new File("src/test/resources/testproperties1.properties");
    FileOutputStream out;
    FileOutputStream out1;
    String lineSep = System.getProperty("line.separator");
    String state0 = "state0_property = 0"+lineSep;
    String state1 = "state1_property = 1"+lineSep;
    String state2 = "state2_property = 2"+lineSep;

    DynamicProperties props;
    volatile int state;
    
    @Before
    public void setup() throws Exception {
        props = new DynamicProperties(directory);
        props.addSimpleNotificationListener(this);
        state = 0;
        Thread.sleep(500L);
        
        assertFalse(testFile.exists());
        assertFalse(testFile1.exists());
        
        out = new FileOutputStream(testFile);
        out.write(state0.getBytes());
        out.flush();
        Thread.sleep(500L);
    }
    
    @After 
    public void teardown() throws Throwable {
        Throwable error = null;
        try {
            out.close();
            out1.close();
        } catch (Throwable t) {
            error = t;
        }
        boolean delete = testFile.delete();
        boolean delete1 = testFile1.delete();
        assertTrue(delete && delete1);
        if (error != null)
            throw error;
    }
    
    @Test(timeout = 5000)
    public void test() {
        while(state < 3) 
            ;
    }

    @Override
    public void notified() {
        try {
            switch (state) {
            case 0:
                state0();
                break;
            case 1:
                state1();
                break;
            case 2:
                state2();
            default:
                break;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void state0() throws Exception {
        assertEquals("0", props.properties().getProperty("state0_property"));
        out1 = new FileOutputStream(testFile1);
        out1.write(state1.getBytes()); 
        out1.flush();
        state = 1;
    }
   
    public void state1() throws Exception {
        assertEquals("0", props.properties().getProperty("state0_property"));
        assertEquals("1", props.properties().getProperty("state1_property"));
        out.write(state2.getBytes());
        out.flush();
        state = 2;
    }
    
    public void state2() {
        assertEquals("0", props.properties().getProperty("state0_property"));
        assertEquals("1", props.properties().getProperty("state1_property"));
        assertEquals("2", props.properties().getProperty("state2_property"));
        state = 3;
    }
}
