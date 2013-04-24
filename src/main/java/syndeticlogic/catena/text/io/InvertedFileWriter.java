package syndeticlogic.catena.text.io;

import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;

import syndeticlogic.catena.text.postings.InvertedList;
import syndeticlogic.catena.text.postings.InvertedListDescriptor;

public interface InvertedFileWriter {
    void open(String fileName);
    void close();
    long writeFile(SortedMap<String, InvertedList> postings, List<InvertedListDescriptor> invertedListDescriptor);
    void setBlockSize(int blockSize);
    int getBlockSize();
}
