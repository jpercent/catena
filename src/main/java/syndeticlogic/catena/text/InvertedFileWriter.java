package syndeticlogic.catena.text;

import java.util.Map;
import java.util.SortedMap;

public interface InvertedFileWriter {
	long writeFile(String name, SortedMap<String, InvertedList> postings, Map<Integer, Long> wordToOffset);
}
