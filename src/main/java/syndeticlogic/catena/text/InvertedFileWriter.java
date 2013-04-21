package syndeticlogic.catena.text;

import java.util.List;
import java.util.SortedMap;

public interface InvertedFileWriter {
    void open(String fileName);
    void close();
    long writeFile(SortedMap<String, InvertedList> postings, List<InvertedListDescriptor> invertedListDescriptor);
    void setBlockSize(int blockSize);
    int getBlockSize();
}
