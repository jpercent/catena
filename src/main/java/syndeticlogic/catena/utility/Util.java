package syndeticlogic.catena.utility;

import java.io.File;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map;

public class Util {
	public static String prefixToPath(String prefix) {
		if(!prefix.equals("")) {
			prefix += System.getProperty("file.separator");
		}
		return prefix;
	}
	public static String createPath(String ...strings) {
		String ret = "";
		for(String string : strings) {
			ret += string + System.getProperty("file.separator");
		}
		return ret;
	}

	public static boolean delete(String fileName) throws Exception {
		File f = new File(fileName);
		if (!f.exists())
			return true;

		if (f.isDirectory()) {

			String[] files = f.list();
			for (String file : files) {
				if (!delete(file))
					return false;
			}
		}
		return f.delete();
	}
	
	public static double freeMemoryPercentage() {
        Runtime runtime = Runtime.getRuntime();
        double freeMemory = (double) runtime.freeMemory();
        double totalMemory = (double) runtime.totalMemory();
        double percentFree = freeMemory / totalMemory * 100.0;
        return percentFree;
    }

	public static String shortName(@SuppressWarnings("rawtypes") Class klass) {
		String className = klass.getName();
		int lastDot = className.lastIndexOf('.');

		if (lastDot != -1) {
			className = className.substring(lastDot + 1);
		}
		return className;
	}
	
    private static final float DEFAULT_LOAD_FACTOR = 0.5f;
    private static final int[] PRIMES = {// 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 37, 43, 53, 59, 67, 79, 89, 101, 113, 127, 149,
        167, 191, 223, 251, 281, 313, 349, 389, 433, 487, 541, 601, 673, 751, 839, 937, 1049,
             1171, 1301, 1447, 1607, 1787, 1987, 2207, 2459, 2731, 3037, 3373, 3761, 4177, 4637,
             5153, 5737, 6373, 7079, 7867, 8737, 9719, 10789, 11981, 13309, 14779, 16411, 18217,
             20231, 22469, 24943, 27689, 30757, 34141, 37897, 42071, 46703, 51853, 57557, 63901,
             70937, 78779, 87473, 97103, 107791, 119653, 132817, 147449, 163673, 181693, 201683,
             223903, 248533, 275881, 306239, 339943, 377339, 418849, 464923, 516077, 572867, 635891,
             705841, 783487, 869683, 965357, 1071563, 1189453, 1320301, 1465547, 1626763, 1805729,
             2004377, 2224861, 2469629, 2741303, 3042857, 3377579, 3749117, 4161527, 4619309,
             5127433, 5691457, 6317527, 7012469, 7783843, 8640109, 9590531, 10645507, 11816521,
             13116343, 14559151, 16160663, 17938357, 19911581, 22101889, 24533099, 27231751,
             30227287, 33552293, 37243051, 41339843, Integer.MAX_VALUE};

    public static Map<Object, Object> createMap(int initialCapacity)
    {
    	int value = Arrays.binarySearch(PRIMES, initialCapacity);
        if (value < 0) {
            value = -value - 1;
        }
        return new Hashtable<Object, Object>(PRIMES[value], DEFAULT_LOAD_FACTOR);
    }	
}
