package syndeticlogic.catena.utility;


import java.util.Properties;

import static org.junit.Assert.*;

import org.junit.Test;

import syndeticlogic.catena.utility.PropertiesUtility;

public class PropUtilityTest {

    @Test
    public void setUp() throws Exception {
        Properties p = PropertiesUtility.load(PropertiesUtility.CONFIG_PROPERTIES);
        String prefix = p.getProperty(PropertiesUtility.CONFIG_BASE_DIRECTORY);
        assertEquals("target/arrays/", prefix);
    }

}
