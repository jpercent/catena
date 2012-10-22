package syndeticlogic.catena.utility;

import static org.junit.Assert.*;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DynamicClassLoaderTest implements SimpleNotificationListener {
    
    String baseDirectory = FilenameUtils.concat(FilenameUtils.concat(FilenameUtils.concat(new File(".").getAbsolutePath(), "src"), "test"), "resources");
    String loaderDirectory = FilenameUtils.concat(baseDirectory, "classes");
    String jar = "hive-cli-0.7.0-cdh3u0.jar";
    String jar1 = "hive-jdbc-0.7.0-cdh3u0.jar";
    File loaderDirectoryFile = new File(loaderDirectory);
    File jarFile = new File(FilenameUtils.concat(baseDirectory, jar));
    File copiedJarFile = new File(FilenameUtils.concat(loaderDirectory, jar));
    File jarFile1 = new File(FilenameUtils.concat(baseDirectory, jar1));
    File copiedJarFile1 = new File(FilenameUtils.concat(loaderDirectory, jar1));
    String state0Name = "org.apache.hadoop.hive.cli.CliDriver";
    String state1Name = "org.apache.hadoop.hive.jdbc.HiveDriver";
    DynamicClassLoader loader;
    volatile int state;
    
    @Before
    public void setup() throws Exception {
        loader = new DynamicClassLoader(loaderDirectory);
        loader.addSimpleNotificationListener(this);
        boolean exceptionCaught = false;
        try {
            loader.loader().loadClass(state0Name);
        } catch(Exception e){
            exceptionCaught = true;
        }
        assertTrue(exceptionCaught);
        exceptionCaught = false;
        try {
            loader.loader().loadClass(state1Name);
        } catch(Exception e){
            exceptionCaught = true;            
        }
        assertTrue(exceptionCaught);
        state = 0;
        assertFalse(copiedJarFile.exists());
        assertFalse(copiedJarFile1.exists());
        FileUtils.copyFileToDirectory(jarFile, loaderDirectoryFile);
        assertTrue(copiedJarFile.exists());
        
        Thread.sleep(500L);
    }
    
    @After 
    public void teardown() throws Throwable {
        boolean delete = copiedJarFile.delete();
        boolean delete1 = copiedJarFile1.delete();
        assertTrue(delete && delete1);
    }
    
    @Test(timeout = 5000)
    public void test() {
        while(state < 2) 
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
            default:
                break;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void state0() throws Exception {
        Class<?> clazz  = loader.loader().loadClass(state0Name);
        assertEquals(state0Name, clazz.getName());
        FileUtils.copyFileToDirectory(jarFile1, loaderDirectoryFile);
        state = 1;
    }
   
    public void state1() throws Exception {
        Class<?> clazz  = loader.loader().loadClass(state0Name);
        assertEquals(state0Name, clazz.getName());
        clazz  = loader.loader().loadClass(state1Name);
        assertEquals(state1Name, clazz.getName());
        FileUtils.copyFileToDirectory(jarFile1, loaderDirectoryFile);
        state = 2;        
    }
}
