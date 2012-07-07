package syndeticlogic.catena.utility;

import java.util.Properties;
import java.io.File;
import java.io.IOException;
import java.io.FileInputStream;
import java.net.URL;

public class PropertiesUtility {
    public static final String CONFIG_BASE_DIRECTORY = "base_directory";
    public static final String CONFIG_PROPERTIES = "catena-config.properties";
    public static final String SPLIT_THRESHOLD = "split_threshold";
    
    private PropertiesUtility() {  }

    public static Properties load(String propsName) throws Exception {
        Properties props = new Properties();
        URL url = ClassLoader.getSystemResource(propsName);
        props.load(url.openStream());
        return props;
    }

    public static Properties load(File propsFile) throws IOException {
        Properties props = new Properties();
        FileInputStream fis = new FileInputStream(propsFile);
        props.load(fis);    
        fis.close();
        return props;
    }
}

