package syndeticlogic.catena.utility;


import java.util.Properties;

import static org.junit.Assert.*;

import org.junit.Test;

import syndeticlogic.catena.utility.PropUtility;

public class PropUtilityTest {

    @Test
    public void setUp() throws Exception {
        Properties p = PropUtility.load(PropUtility.CONFIG_PROPERTIES);
        String prefix = p.getProperty(PropUtility.CONFIG_BASE_DIRECTORY);
        assertEquals("target/arrays/", prefix);
    }

}
