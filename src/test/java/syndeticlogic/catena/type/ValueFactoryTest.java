package syndeticlogic.catena.type;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import syndeticlogic.catena.utility.Codec;
import syndeticlogic.catena.utility.DynamicClassLoader;
import syndeticlogic.catena.utility.DynamicProperties;
import syndeticlogic.catena.type.IntegerValue;

public class ValueFactoryTest {    
    String baseDirectory = FilenameUtils.concat(FilenameUtils.concat(FilenameUtils.concat(new File(".").getAbsolutePath(), "src"), "test"), "resources");
    String classesDirectory = FilenameUtils.concat(baseDirectory, "classes");
    String propertiesDirectory = FilenameUtils.concat(baseDirectory, "properties");
    String jar = "syndeticlogic-catena-1.0-SNAPSHOT.jar";
    File classesDirectoryFile = new File(classesDirectory);
    File jarFile = new File(FilenameUtils.concat(baseDirectory, jar));
    File copiedJarFile = new File(FilenameUtils.concat(classesDirectory, jar));

    DynamicClassLoader loader;
    DynamicProperties properties;
    ValueFactory valueFactory;
    
    FileOutputStream out;
    File testFile = new File(FilenameUtils.concat(propertiesDirectory, "testproperties.properties"));
    String userDefinedClassName = "syndeticlogic.catena.type.TestValue";
        
    @Before
    public void setup() throws Exception {
        loader = new DynamicClassLoader(classesDirectory);
        properties = new DynamicProperties(propertiesDirectory);
        assertFalse(testFile.exists());

        boolean exceptionCaught = false;
        try {
            loader.loader().loadClass(userDefinedClassName);
        } catch(Exception e){
            exceptionCaught = true;
        }
        assertTrue(exceptionCaught);
        exceptionCaught = false;
        try {
            loader.loader().loadClass(userDefinedClassName);
        } catch(Exception e){
            exceptionCaught = true;            
        }
        assertTrue(exceptionCaught);
        assertFalse(copiedJarFile.exists());        
        Thread.sleep(500L);
        valueFactory = new ValueFactory(properties, loader);
        ClassLoader.getSystemClassLoader().loadClass("syndeticlogic.catena.type.IntegerValue");
        Class<?> intValue = Class.forName("syndeticlogic.catena.type.IntegerValue");
        assertEquals("syndeticlogic.catena.type.IntegerValue", intValue.getName());   
        
        
        
        FileUtils.copyFileToDirectory(jarFile, classesDirectoryFile);
        assertTrue(copiedJarFile.exists());
        Thread.sleep(500);        
        Properties props = new Properties();
        props.setProperty(userDefinedClassName, valueFactory.claimNextOrdinal());
        out = new FileOutputStream(testFile);
        props.store(out, "test properties");
        out.flush();
        Thread.sleep(1000);
    }
    
    @After 
    public void teardown() throws Exception {
        loader=null;
        properties=null;
        valueFactory=null;
        Exception error=null;
        try {
            out.close();
        } catch(Exception e) {
            error = e;
        }
        boolean delete = testFile.delete();
        boolean delete1 = copiedJarFile.delete();
        assertTrue(delete && delete1);
        if(error != null) {
            throw error;
        }
    }

    @Test
    public void test() throws Throwable {
        Codec.configureCodec(valueFactory);
        IntegerValue intValue = (IntegerValue) valueFactory.getValue("syndeticlogic.catena.type.IntegerValue");
        assertTrue(intValue != null);
        byte[] dest = new byte[Type.INTEGER.length()];
        assertEquals(Type.INTEGER.length(), Codec.getCodec().encode(42, dest, 0));
        intValue.reset(dest, 0, dest.length);        
        Integer integer = (Integer) intValue.objectize();
        assertEquals(42, integer.intValue());
        
        Value test = valueFactory.getValue(userDefinedClassName);
        assertEquals("objectized", test.objectize());
    }
}
