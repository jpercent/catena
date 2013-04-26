package syndeticlogic.catena.text.io;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.text.io.WriteCursor.BaseWriteCursor;
import syndeticlogic.catena.text.postings.InvertedList;
import syndeticlogic.catena.text.postings.InvertedListDescriptor;

public class InvertedFileWriteCursor extends BaseWriteCursor {
    private static final Log log = LogFactory.getLog(InvertedFileWriteCursor.class);
    private Iterator<Map.Entry<String, InvertedList>> postings;
    private List<InvertedListDescriptor> descriptors;
    private InvertedList currentList;
    private long fileOffset;

    public InvertedFileWriteCursor(SortedMap<String, InvertedList> postings) {
        this.postings = postings.entrySet().iterator();
        this.descriptors = new ArrayList<InvertedListDescriptor>(postings.size());
        fileOffset = 0;
    }
    
    public List<InvertedListDescriptor> getInvertedListDescriptors() {
        return descriptors;
    }
    
    @Override
    public boolean hasNext() {
        return postings.hasNext();
    }

    @Override
    public int nextLength() {
        currentList = postings.next().getValue();
        return currentList.size();
    }

    @Override
    public int encodeNext(byte[] buffer, int offset) {
        int length = currentList.encode(buffer, offset);
        descriptors.add(new InvertedListDescriptor(currentList.getWord(), fileOffset, length, currentList.getDocumentFrequency()));
        fileOffset += length;
        return length;
    }
}
