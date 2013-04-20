package syndeticlogic.catena.text;

import java.util.LinkedList;
import java.util.SortedMap;

public interface InvertedFileWriter {
    void open(String fileName);
    void close();
    long write(SortedMap<String, InvertedList> postings, LinkedList<InvertedListDescriptor> invertedListDescriptor);
}
